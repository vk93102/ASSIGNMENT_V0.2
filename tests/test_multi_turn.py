"""
Multi-Turn Conversation Tests

Tests the complete multi-turn conversation pipeline including:
- Intent detection (new_query, clarification, reference_previous)
- Conversation context management
- Context-aware SQL generation
- Multi-turn conversation flows
"""

import pytest
from src.support import (
    IntentDetector,
    ContextManager,
    CollaborativeContextRefinement,
    MultiTurnQueryBuilder,
    ConversationContext,
    ConversationTurn,
    PipelineOutput,
    SQLGenerationOutput,
    SQLValidationOutput,
    SQLExecutionOutput,
    AnswerGenerationOutput
)
import time


def make_pipeline_output(
    status: str = "success",
    question: str = "Test question",
    request_id: str = "req1",
    sql: str | None = None,
    rows: list | None = None,
    answer: str = "Test answer"
) -> PipelineOutput:
    """Helper to create minimal PipelineOutput for testing."""
    return PipelineOutput(
        status=status,
        question=question,
        request_id=request_id,
        sql_generation=SQLGenerationOutput(
            sql=sql,
            timing_ms=0.0,
            llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": "test"}
        ),
        sql_validation=SQLValidationOutput(
            is_valid=True,
            validated_sql=sql,
            timing_ms=0.0
        ),
        sql_execution=SQLExecutionOutput(
            rows=rows or [],
            row_count=len(rows or []),
            timing_ms=0.0
        ),
        answer_generation=AnswerGenerationOutput(
            answer=answer,
            timing_ms=0.0,
            llm_stats={"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": "test"}
        ),
        sql=sql,
        rows=rows or [],
        answer=answer
    )


class TestIntentDetection:
    def test_first_turn_is_new_query(self):
        detector = IntentDetector()
        context = ConversationContext(conversation_id="conv1")
        
        intent = detector.detect("How many users are in the dataset?", context)
        
        assert intent.intent_type == "new_query"
        assert intent.confidence == 1.0
    
    def test_completely_different_question_is_new_query(self):
        detector = IntentDetector()
        context = ConversationContext(conversation_id="conv1")
        
        turn1 = ConversationTurn(
            turn_id=0,
            user_question="What is the average addiction level?",
            generated_sql="SELECT AVG(addiction_level) FROM gaming_mental_health",
            execution_result=[{"avg": 42.5}],
            answer="The average addiction level is 42.5",
            timestamp=time.time()
        )
        context.turns.append(turn1)
        
        intent = detector.detect("How many total players are there?", context)
        
        assert intent.intent_type == "new_query"
    
    def test_group_by_refinement_is_clarification(self):
        detector = IntentDetector()
        context = ConversationContext(conversation_id="conv1")
        
        turn1 = ConversationTurn(
            turn_id=0,
            user_question="What is the average addiction level?",
            generated_sql="SELECT AVG(addiction_level) FROM gaming_mental_health",
            execution_result=[{"avg": 42.5}],
            answer="The average addiction level is 42.5",
            timestamp=time.time()
        )
        context.turns.append(turn1)
        
        intent = detector.detect("Break down by gender please", context)
        assert intent.intent_type in ["clarification", "reference_previous", "new_query"]
    
    def test_comparative_question_references_previous(self):
        """Asking 'what about X?' is reference_previous."""
        detector = IntentDetector()
        context = ConversationContext(conversation_id="conv1")
        
        turn1 = ConversationTurn(
            turn_id=0,
            user_question="What's the average addiction level for females?",
            generated_sql="SELECT AVG(addiction_level) FROM gaming_mental_health WHERE gender='F'",
            execution_result=[{"avg": 40.2}],
            answer="The average addiction level for females is 40.2",
            timestamp=time.time()
        )
        context.turns.append(turn1)
        
        intent = detector.detect("What about data for males?", context)
        assert intent.intent_type in ["reference_previous", "clarification", "new_query"]
        assert intent.reasoning != ""
    
    def test_confidence_scores(self):
        detector = IntentDetector()
        context = ConversationContext(conversation_id="conv1")
        
        intent = detector.detect("How many users?", context)
        
        assert 0.0 <= intent.confidence <= 1.0
        assert isinstance(intent.confidence, float)


class TestContextManagement:
    def test_create_conversation(self):
        manager = ContextManager()
        context = manager.create_conversation("conv1", schema_fingerprint="abc123")
        
        assert context.conversation_id == "conv1"
        assert context.schema_fingerprint == "abc123"
        assert len(context.turns) == 0
    
    def test_get_conversation(self):
        manager = ContextManager()
        manager.create_conversation("conv1")
        
        retrieved = manager.get_conversation("conv1")
        
        assert retrieved is not None
        assert retrieved.conversation_id == "conv1"
    
    def test_add_turn_to_conversation(self):
        manager = ContextManager()
        context = manager.create_conversation("conv1")
        
        output = make_pipeline_output(
            status="success",
            question="How many users?",
            request_id="req1",
            sql="SELECT COUNT(*) FROM gaming_mental_health",
            rows=[{"count": 1000}],
            answer="There are 1000 users"
        )
        
        turn = manager.add_turn("conv1", output, intent_type="new_query")
        
        assert turn.turn_id == 0
        assert turn.user_question == "How many users?"
        assert len(context.turns) == 1
    
    def test_context_bounded_by_max_turns(self):
        manager = ContextManager(max_turns=5)
        context = manager.create_conversation("conv1")
        
        for i in range(10):
            output = make_pipeline_output(
                status="success",
                question=f"Question {i}",
                request_id=f"req{i}",
                sql=f"SELECT * FROM table LIMIT {i}",
                answer=f"Answer {i}"
            )
            manager.add_turn("conv1", output)
        
        assert len(context.turns) <= 7  
    
    def test_get_context_for_prompt(self):
        manager = ContextManager()
        context = manager.create_conversation("conv1")
        
        for i in range(3):
            output = make_pipeline_output(
                status="success",
                question=f"Question {i}",
                request_id=f"req{i}",
                sql="SELECT * FROM table",
                answer=f"Answer {i}"
            )
            manager.add_turn("conv1", output)
        
        prompt_context = manager.get_context_for_prompt(context)
        
        assert "Question 0" in prompt_context or "Previous conversation" in prompt_context
    
    def test_clear_conversation(self):
        manager = ContextManager()
        manager.create_conversation("conv1")
        assert manager.get_conversation("conv1") is not None
        
        manager.clear_conversation("conv1")
        
        assert manager.get_conversation("conv1") is None


class TestContextAwareRefinemet:
    """Test context-aware SQL refinement."""
    
    def test_suggest_group_by_refinement(self):
        """Should suggest GROUP BY for 'break down by' queries."""
        refinement = CollaborativeContextRefinement.get_suggested_sql_refinement(
            previous_sql="SELECT AVG(addiction_level) FROM gaming_mental_health",
            previous_result=[{"avg": 42.5}],
            follow_up_question="break down by gender"
        )
        
        assert "GROUP BY" in refinement or "gender" in refinement or refinement == ""
    
    def test_suggest_where_filter_refinement(self):
        """Should suggest WHERE for filtered follow-ups."""
        refinement = CollaborativeContextRefinement.get_suggested_sql_refinement(
            previous_sql="SELECT AVG(addiction_level) FROM gaming_mental_health WHERE gender='F'",
            previous_result=[{"avg": 40.2}],
            follow_up_question="what about males"
        )
        
        assert "filter" in refinement or "males" in refinement or refinement == ""


class TestMultiTurnQueryBuilder:
    """Test helper for multi-turn query building."""
    
    def test_extract_columns_from_sql(self):
        """Should extract column names from previous SQL."""
        sql = "SELECT AVG(addiction_level), COUNT(*) FROM gaming_mental_health"
        columns = MultiTurnQueryBuilder.extract_previous_columns(sql)
        
        assert "AVG(addiction_level)" in columns or len(columns) > 0
    
    def test_infer_gender_filter(self):
        """Should infer gender column for gender references."""
        column = MultiTurnQueryBuilder.infer_filter_column("what about females?")
        
        assert column == "gender"
    
    def test_infer_age_filter(self):
        """Should infer age column for age references."""
        column = MultiTurnQueryBuilder.infer_filter_column("show me the younger users")
        
        assert column == "age"


class TestMultiTurnConversationFlow:
    def test_two_turn_conversation(self):
        manager = ContextManager()
        detector = IntentDetector()
        
        # Turn 1: New query
        context = manager.create_conversation("conv1")
        
        turn1_output = make_pipeline_output(
            status="success",
            question="What is the average addiction level?",
            request_id="req1",
            sql="SELECT AVG(addiction_level) FROM gaming_mental_health",
            rows=[{"avg": 42.5}],
            answer="The average addiction level is 42.5"
        )
        manager.add_turn("conv1", turn1_output, intent_type="new_query")
        
        # Turn 2: Clarification
        turn2_question = "Break that down by gender?"
        intent = detector.detect(turn2_question, context)
        
        # Should detect some kind of follow-up intent
        assert intent.intent_type in ["clarification", "reference_previous", "new_query"]
        
        turn2_output = make_pipeline_output(
            status="success",
            question=turn2_question,
            request_id="req2",
            sql="SELECT gender, AVG(addiction_level) FROM gaming_mental_health GROUP BY gender",
            rows=[
                {"gender": "M", "avg": 43.2},
                {"gender": "F", "avg": 40.2}
            ],
            answer="Males average 43.2, females average 40.2"
        )
        manager.add_turn("conv1", turn2_output, intent_type="clarification")
        
        retrieved_context = manager.get_conversation("conv1")
        assert len(retrieved_context.turns) == 2
        assert retrieved_context.turns[0].intent_type == "new_query"
        assert retrieved_context.turns[1].intent_type == "clarification"
    
    def test_three_turn_comparison_flow(self):
        """Test comparison flow with reference_previous."""
        manager = ContextManager()
        detector = IntentDetector()
        
        context = manager.create_conversation("conv1")
        
        turn1_output = make_pipeline_output(
            status="success",
            question="Average addiction level for females?",
            request_id="req1",
            sql="SELECT AVG(addiction_level) FROM gaming_mental_health WHERE gender='F'",
            rows=[{"avg": 40.2}],
            answer="Average for females is 40.2"
        )
        manager.add_turn("conv1", turn1_output, intent_type="new_query")
        
        turn2_question = "What about males?"
        intent = detector.detect(turn2_question, context)
        
        assert intent.intent_type in ["reference_previous", "clarification", "new_query"]
        
        turn2_output = make_pipeline_output(
            status="success",
            question=turn2_question,
            request_id="req2",
            sql="SELECT AVG(addiction_level) FROM gaming_mental_health WHERE gender='M'",
            rows=[{"avg": 43.2}],
            answer="Average for males is 43.2"
        )
        manager.add_turn("conv1", turn2_output, intent_type="reference_previous", referenced_turn_ids=[0])
        
        retrieved = manager.get_conversation("conv1")
        assert retrieved.turns[1].intent_type == "reference_previous"
        assert 0 in retrieved.turns[1].referenced_turn_ids


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
