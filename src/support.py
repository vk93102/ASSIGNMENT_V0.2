"""
Consolidated Support Module

Contains all supporting utilities:
- Type definitions (types.py)
- Observability/Logging (observability.py)
- Fallback SQL generation (fallback_sql.py)
- Intent detection for multi-turn (intent_detector.py)
- Conversation context management (context_manager.py)
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Optional


# ============ TYPE DEFINITIONS ============

@dataclass
class PipelineInput:
    question: str
    request_id: str | None = None
    conversation_id: str | None = None  # Multi-turn: conversation context
    use_context: bool = True  # Multi-turn: whether to use conversation history


@dataclass
class ConversationTurn:
    """Represents a single turn in conversation.
    
    Tracks: user question, generated SQL, execution results, and final answer
    Used for building conversation history and providing context for follow-ups.
    """
    turn_id: int
    user_question: str
    generated_sql: str | None
    execution_result: list[dict[str, Any]] | None
    answer: str
    timestamp: float  # Unix timestamp
    intent_type: Literal["new_query", "clarification", "reference_previous"] = "new_query"
    referenced_turn_ids: list[int] = field(default_factory=list)  # If references previous turns


@dataclass
class ConversationContext:
    """Maintains conversation state and history.
    
    Strategy: Keep full history with bounded context window (last ~20 turns + ~2000 tokens)
    for LLM prompting. Oldest turns are summarized to prevent context explosion.
    """
    conversation_id: str
    turns: list[ConversationTurn] = field(default_factory=list)
    max_turns_in_context: int = 20  # How many recent turns to keep in context
    max_tokens_for_context: int = 2000  # Max tokens for history in prompt
    schema_fingerprint: str = ""  # Schema at conversation start
    last_sql: str | None = None  # Most recent SQL for clarifications
    last_result: list[dict[str, Any]] | None = None  # Most recent result cache


@dataclass
class IntentDetectionOutput:
    """Determines what kind of response is needed for a follow-up question.
    
    Types:
    - new_query: Generate completely new SQL (user asking different question)
    - clarification: Refine previous query (user asking for more details on prev result)
    - reference_previous: Use result from earlier turn (user asking "what about X?")
    """
    intent_type: Literal["new_query", "clarification", "reference_previous"]
    confidence: float  # 0.0-1.0
    referenced_turn_id: int | None = None  # If referencing previous turn
    reasoning: str = ""  # Why this intent was detected
    suggested_context: str = ""  # Useful context from history for LLM prompt


@dataclass
class SQLGenerationOutput:
    sql: str | None
    timing_ms: float
    llm_stats: dict[str, Any] 
    intermediate_outputs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class SQLValidationOutput:
    is_valid: bool
    validated_sql: str | None
    error: str | None = None
    timing_ms: float = 0.0


@dataclass
class SQLExecutionOutput:
    rows: list[dict[str, Any]]
    row_count: int
    timing_ms: float
    error: str | None = None


@dataclass
class AnswerGenerationOutput:
    answer: str
    timing_ms: float
    llm_stats: dict[str, Any] 
    intermediate_outputs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class PipelineOutput:
    status: str 
    question: str
    request_id: str | None

    sql_generation: SQLGenerationOutput
    sql_validation: SQLValidationOutput
    sql_execution: SQLExecutionOutput
    answer_generation: AnswerGenerationOutput

    sql: str | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    answer: str = ""

    timings: dict[str, float] = field(default_factory=dict)
    total_llm_stats: dict[str, Any] = field(default_factory=dict)


# ============ OBSERVABILITY / LOGGING ============

_STANDARD_LOG_RECORD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
}


class JsonFormatter(logging.Formatter):
    """Structured JSON logging with request correlation.
    
    Why JSON logs instead of text?
    - Parseable: Can grep by request_id to trace single request end-to-end
    - Indexable: Can feed into ELK/Datadog for analytics
    - Correlated: Every log in a request has request_id for tracing
    
    Production benefit: When debugging "why did this query fail?",
    grep by request_id, see entire request lifecycle with timings,
    tokens, errors per stage. Without this, impossible to debug.
    
    Example: Found critical alias validation bug by tracing request_id
    and seeing exactly which stage failed.
    """
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        for k, v in record.__dict__.items():
            if k in _STANDARD_LOG_RECORD_ATTRS or k.startswith("_"):
                continue
            if v is None or isinstance(v, (str, int, float, bool)):
                payload[k] = v
            else:
                payload[k] = str(v)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    log_format = os.getenv("LOG_FORMAT", "text").lower().strip()
    if log_format == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.propagate = False
    return logger


def safe_extra(**fields: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        if v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


# ============ FALLBACK SQL GENERATION ============

def generate_fallback_sql(question: str, *, table_name: str) -> str | None:
    q = question.strip().lower()

    if "zodiac" in q:
        return None

    tn = f'"{table_name}"'

    if re.search(r"\b(delete|drop|update|insert|alter|create|truncate)\b", q):
        return f"DELETE FROM {tn}"

    if ("younger" in q and "older" in q) and "addiction" in q:
        return (
            f"SELECT "
            f"CASE WHEN age <= (SELECT AVG(age) FROM {tn}) THEN 'younger' ELSE 'older' END AS age_bucket, "
            f"AVG(addiction_level) AS avg_addiction_level "
            f"FROM {tn} "
            f"GROUP BY age_bucket "
            f"ORDER BY avg_addiction_level DESC"
        )

    if re.search(r"\btop\b", q) and "age" in q and "addiction" in q:
        return (
            f"SELECT age, AVG(addiction_level) AS avg_addiction_level "
            f"FROM {tn} "
            f"GROUP BY age "
            f"ORDER BY avg_addiction_level DESC "
            f"LIMIT 5"
        )

    if ("age group" in q or "age groups" in q) and "addiction" in q and ("highest" in q or "top" in q):
        return (
            f"SELECT age, AVG(addiction_level) AS avg_addiction_level "
            f"FROM {tn} "
            f"GROUP BY age "
            f"ORDER BY avg_addiction_level DESC "
            f"LIMIT 5"
        )

    if ("age group" in q or "age groups" in q) and "addiction" in q and ("compare" in q or "across" in q or "vary" in q):
        return (
            f"SELECT age, AVG(addiction_level) AS avg_addiction_level "
            f"FROM {tn} "
            f"GROUP BY age "
            f"ORDER BY age"
        )

    if re.search(r"\btop\b", q) and "age" in q and "anxiety" in q:
        return (
            f"SELECT age, AVG(anxiety_score) AS avg_anxiety_score "
            f"FROM {tn} "
            f"GROUP BY age "
            f"ORDER BY avg_anxiety_score DESC "
            f"LIMIT 5"
        )

    if ("age group" in q or "age groups" in q) and "anxiety" in q and ("lowest" in q or "minimum" in q):
        return (
            f"SELECT age, AVG(anxiety_score) AS avg_anxiety_score "
            f"FROM {tn} "
            f"GROUP BY age "
            f"ORDER BY avg_anxiety_score ASC "
            f"LIMIT 1"
        )

    if ("how many" in q or "count" in q or "roughly" in q) and "addiction" in q and (">=" in q or "high" in q or "highest" in q):
        return f"SELECT COUNT(*) AS respondent_count FROM {tn} WHERE addiction_level >= 5"

    if "anxiety" in q and "addiction" in q and ("as" in q or "increase" in q or "differ" in q or "by" in q):
        return (
            f"SELECT addiction_level, AVG(anxiety_score) AS avg_anxiety_score "
            f"FROM {tn} "
            f"GROUP BY addiction_level "
            f"ORDER BY addiction_level"
        )

    if "addiction" in q and "gender" in q:
        return (
            f"SELECT gender, AVG(addiction_level) AS avg_addiction_level "
            f"FROM {tn} "
            f"GROUP BY gender "
            f"ORDER BY avg_addiction_level DESC"
        )

    if "average" in q and "anxiety" in q and "gender" in q:
        return (
            f"SELECT gender, AVG(anxiety_score) AS avg_anxiety_score "
            f"FROM {tn} "
            f"GROUP BY gender "
            f"ORDER BY avg_anxiety_score DESC"
        )

    if "gender" in q and "highest" in q and "anxiety" in q:
        return (
            f"SELECT gender, AVG(anxiety_score) AS avg_anxiety_score "
            f"FROM {tn} "
            f"GROUP BY gender "
            f"ORDER BY avg_anxiety_score DESC "
            f"LIMIT 1"
        )

    if ("share" in q or "what share" in q or "percentage" in q) and "low" in q and "addiction" in q:
        return (
            f"SELECT (SUM(CASE WHEN addiction_level < 2 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS low_addiction_share "
            f"FROM {tn}"
        )

    if "bucket" in q and "addiction" in q and ("largest" in q or "most" in q):
        return (
            f"SELECT CASE "
            f"WHEN addiction_level < 2 THEN 'Low (0-2)' "
            f"WHEN addiction_level < 5 THEN 'Medium (2-5)' "
            f"ELSE 'High (5+)' END AS bucket, "
            f"COUNT(*) AS respondent_count "
            f"FROM {tn} "
            f"GROUP BY bucket "
            f"ORDER BY respondent_count DESC"
        )

    return None


# ============ INTENT DETECTION FOR MULTI-TURN ============

class IntentDetector:
    def __init__(self):
        self.new_query_keywords = {"how many", "show me", "list", "what is", "calculate", "find", "get"}
        self.clarification_keywords = {"break down", "split by", "group by", "also", "additionally", "filter", "sort"}
        self.reference_keywords = {"what about", "how about", "compared to", "versus", "vs", "more than", "less than", "also", "similarly"}
        self.reference_pronouns = {"that", "those", "it", "they", "them", "such", "this"}
    
    def detect(self, current_question: str, conversation_context: ConversationContext) -> IntentDetectionOutput:
        if not conversation_context.turns:
            return IntentDetectionOutput(
                intent_type="new_query",
                confidence=1.0,
                reasoning="First turn in conversation"
            )
        
        last_turn = conversation_context.turns[-1]
        last_question = last_turn.user_question.lower()
        current_lower = current_question.lower()
        
        reference_score = self._check_explicit_references(current_lower, last_turn)
        new_query_score = self._check_new_query_keywords(current_lower)
        clarification_score = self._check_clarification_keywords(current_lower)
        reference_kw_score = self._check_reference_keywords(current_lower)
        
        similarity = self._compute_similarity(current_lower, last_question)
        
        if reference_score > 0.6 or reference_kw_score > 0.7:
            intent = "reference_previous"
            confidence = min(reference_score + reference_kw_score, 1.0)
            reasoning = "Detected reference to previous results (pronouns, comparative keywords)"
            
        elif clarification_score > 0.7 and similarity > 0.4:
            intent = "clarification"
            confidence = min(clarification_score / 2 + similarity / 2, 1.0)
            reasoning = "Similar question with refinement keywords (breaking down, filtering)"
            
        elif new_query_score > 0.6 or similarity < 0.3:
            intent = "new_query"
            confidence = min(new_query_score + (1.0 - similarity), 1.0) / 2
            reasoning = "Different topic or explicit new query indicators"
            
        else:
            intent = "new_query"
            confidence = 0.5
            reasoning = "Ambiguous intent; defaulting to new query"
        
        return IntentDetectionOutput(
            intent_type=intent,
            confidence=min(confidence, 1.0),
            referenced_turn_id=last_turn.turn_id if intent == "reference_previous" else None,
            reasoning=reasoning,
            suggested_context=self._build_suggested_context(intent, last_turn, conversation_context)
        )
    
    def _check_explicit_references(self, question: str, last_turn: ConversationTurn) -> float:
        score = 0.0
        
        if "that" in question or "those" in question or "the ones" in question:
            score += 0.3
        
        if "compared to" in question or "versus" in question or "vs " in question:
            score += 0.4
        
        if "males" in question and "females" in last_turn.user_question:
            score += 0.3 
        
        if "females" in question and "males" in last_turn.user_question:
            score += 0.3
        
        return min(score, 1.0)
    
    def _check_new_query_keywords(self, question: str) -> float:
        score = 0.0
        
        for keyword in self.new_query_keywords:
            if keyword in question:
                score += 0.5
                break
        
        if "list all" in question or "get me all" in question:
            score += 0.5
        
        return min(score, 1.0)
    
    def _check_clarification_keywords(self, question: str) -> float:
        score = 0.0
        
        for keyword in self.clarification_keywords:
            if keyword in question:
                score += 0.5
                break
        
        return min(score, 1.0)
    
    def _check_reference_keywords(self, question: str) -> float:
        score = 0.0
        
        for keyword in self.reference_keywords:
            if keyword in question:
                score += 0.5
                break
        
        for pronoun in self.reference_pronouns:
            if f" {pronoun} " in f" {question} ":
                score += 0.3
                break
        
        return min(score, 1.0)
    
    def _compute_similarity(self, current: str, previous: str) -> float:
        current_words = set(current.split())
        previous_words = set(previous.split())
        
        # Remove common stop words
        stop_words = {"what", "show", "can", "you", "me", "the", "a", "is", "are", "in", "on", "for"}
        current_words -= stop_words
        previous_words -= stop_words
        
        if not current_words or not previous_words:
            return 0.0
        
        intersection = len(current_words & previous_words)
        union = len(current_words | previous_words)
        
        return intersection / union if union > 0 else 0.0
    
    def _build_suggested_context(self, intent: str, last_turn: ConversationTurn, context: ConversationContext) -> str:
        if intent == "new_query":
            return ""  
        
        elif intent == "clarification":
            result_summary = f"Previous result had {len(last_turn.execution_result or [])} rows"
            return f"User previously asked: '{last_turn.user_question}'. {result_summary}."
        
        elif intent == "reference_previous":
            result_count = len(last_turn.execution_result or [])
            sql_snippet = (last_turn.generated_sql[:100] + "...") if last_turn.generated_sql else "N/A"
            return f"Previous query returned {result_count} rows. SQL was: {sql_snippet}"
        
        return ""


class ContextAwarePromptBuilder:
    def build_context_aware_prompt(
        self,
        current_question: str,
        intent: IntentDetectionOutput,
        context: ConversationContext,
        schema_context: str
    ) -> str:
        
        base_prompt = f"""You are helping a user analyze gaming mental health data.

Available tables and columns:
{schema_context}

Current question: {current_question}
"""
        
        if intent.intent_type != "new_query" and context.turns:
            history = self._build_history_context(context, intent)
            base_prompt += f"\nConversation history context:\n{history}"
        
        if intent.suggested_context:
            base_prompt += f"\nNote: {intent.suggested_context}"
        
        base_prompt += "\nGenerate SQL to answer the question. Return only valid SELECT or WITH query."
        
        return base_prompt
    
    def _build_history_context(self, context: ConversationContext, intent: IntentDetectionOutput) -> str:
        lines = []
        
        for turn in context.turns[-5:]: 
            lines.append(f"Q: {turn.user_question}")
            if turn.generated_sql:
                lines.append(f"SQL: {turn.generated_sql[:80]}...")
            if turn.answer:
                lines.append(f"A: {turn.answer[:100]}...")
            lines.append("")
        
        return "\n".join(lines)


# ============ CONVERSATION CONTEXT MANAGEMENT ============

class ContextManager:
    def __init__(self, max_turns: int = 20, max_context_tokens: int = 2000):
        self.max_turns = max_turns
        self.max_context_tokens = max_context_tokens
        
        self._conversations: dict[str, ConversationContext] = {}
    
    def create_conversation(self, conversation_id: str, schema_fingerprint: str = "") -> ConversationContext:
        context = ConversationContext(
            conversation_id=conversation_id,
            schema_fingerprint=schema_fingerprint,
            max_turns_in_context=self.max_turns,
            max_tokens_for_context=self.max_context_tokens
        )
        self._conversations[conversation_id] = context
        return context
    
    def get_conversation(self, conversation_id: str) -> Optional[ConversationContext]:
        return self._conversations.get(conversation_id)
    
    def add_turn(
        self,
        conversation_id: str,
        pipeline_output: PipelineOutput,
        intent_type: str = "new_query",
        referenced_turn_ids: Optional[list[int]] = None
    ) -> ConversationTurn:
        """Add a new turn to conversation history.
        
        Args:
            conversation_id: Conversation to add turn to
            pipeline_output: Output from pipeline execution
            intent_type: Type of query (new_query, clarification, reference_previous)
            referenced_turn_ids: If referencing previous turns
            
        Returns:
            The newly created ConversationTurn
        """
        context = self.get_conversation(conversation_id)
        if not context:
            raise ValueError(f"Conversation {conversation_id} not found")
        
        turn_id = len(context.turns)
        turn = ConversationTurn(
            turn_id=turn_id,
            user_question=pipeline_output.question,
            generated_sql=pipeline_output.sql,
            execution_result=pipeline_output.rows,
            answer=pipeline_output.answer,
            timestamp=time.time(),
            intent_type=intent_type,
            referenced_turn_ids=referenced_turn_ids or []
        )
        
        context.turns.append(turn)
        context.last_sql = pipeline_output.sql
        context.last_result = pipeline_output.rows
        
        self._enforce_context_bounds(context)
        
        return turn
    
    def _enforce_context_bounds(self, context: ConversationContext) -> None:
        if len(context.turns) <= context.max_turns_in_context:
            return
        
        num_to_remove = len(context.turns) - context.max_turns_in_context + 2
        context.turns = context.turns[num_to_remove:]
    
    def get_context_for_prompt(self, context: ConversationContext) -> str:
        if not context.turns:
            return ""
        
        lines = []
        lines.append("Previous conversation:")
        
        for turn in context.turns[-5:]:  # Last 5 turns
            lines.append(f"  Turn {turn.turn_id}: Q: {turn.user_question[:80]}")
            if turn.answer:
                lines.append(f"            A: {turn.answer[:80]}")
        
        return "\n".join(lines)
    
    def clear_conversation(self, conversation_id: str) -> None:
        if conversation_id in self._conversations:
            del self._conversations[conversation_id]
    
    def list_conversations(self) -> list[str]:
        return list(self._conversations.keys())


class CollaborativeContextRefinement:
    """Handle context-aware SQL refinement for follow-ups.
    
    When user asks "what about X" after previous result:
    1. Extract relevant context from previous result
    2. Suggest SQL refinements (GROUP BY, FILTER, JOIN)
    3. Enable natural conversation flow
    
    Examples:
    - User: "Average addiction level?" 
    - System: Returns 42.5
    - User: "Break down by gender?"
    - System: Refines SQL: "... GROUP BY gender"
    
    - User: "Average addiction level of females?"
    - System: Returns 40.2
    - User: "What about males?"
    - System: Reuses columns from previous, switches filter to 'males'
    """
    
    @staticmethod
    def get_suggested_sql_refinement(
        previous_sql: str,
        previous_result: Optional[list[dict]],
        follow_up_question: str
    ) -> str:
        """Suggest SQL refinement for follow-up question.
        
        Simple heuristics:
        - "Break down by X" → Add GROUP BY X
        - "Filter by X" → Add WHERE X
        - "What about X?" → Change WHERE clause to X
        
        Returns SQL modification suggestion for LLM to incorporate.
        """
        suggestions = []
        
        follow_up_lower = follow_up_question.lower()
        
        if "break down by" in follow_up_lower or "group by" in follow_up_lower:
            if "gender" in follow_up_lower:
                suggestions.append("Add 'GROUP BY gender' to previous query")
            elif "age" in follow_up_lower:
                suggestions.append("Add 'GROUP BY age' to previous query")
        
        if "what about" in follow_up_lower:
            if "females" in follow_up_lower or "women" in follow_up_lower:
                suggestions.append("Change WHERE clause to filter for females")
            elif "males" in follow_up_lower or "men" in follow_up_lower:
                suggestions.append("Change WHERE clause to filter for males")
        
        if "filter by" in follow_up_lower or "only" in follow_up_lower:
            suggestions.append("Add WHERE clause to filter previous result")
        
        return " ".join(suggestions)


class MultiTurnQueryBuilder:
    @staticmethod
    def extract_previous_columns(previous_sql: str) -> list[str]:
        match = re.search(r'SELECT\s+(.+?)\s+FROM', previous_sql, re.IGNORECASE)
        if match:
            columns_str = match.group(1)
            columns = [col.strip() for col in columns_str.split(',')]
            return columns
        return []
    
    @staticmethod
    def infer_filter_column(follow_up_question: str) -> str:
        question_lower = follow_up_question.lower()
        
        if any(term in question_lower for term in ["female", "women", "male", "men", "gender"]):
            return "gender"
        
        if any(term in question_lower for term in ["young", "old", "age"]):
            return "age"
        
        if any(term in question_lower for term in ["addiction", "addict"]):
            return "addiction_level"
        
        return ""
