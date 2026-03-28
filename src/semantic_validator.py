from __future__ import annotations

import re
from typing import Optional


class SemanticValidator:
    SCHEMA_KEYWORDS = {
        "age", "gender", "addiction_level", "anxiety_score", "hours_played",
        "stress_level", "gaming_mental_health", 
    }

    OUT_OF_DOMAIN_KEYWORDS = {
        "zodiac", "horoscope", "fortune", "lucky", "birthday", "astrology",
        "weather", "stock", "price", "crypto", "bitcoin", "ethereum",
        "time", "date", "today", "tomorrow", "news", "sports", "movie",
        "book", "music", "recipe", "cooking", "travel", "vacation",
        "password", "credit_card", "ssn", "social security", "bank", "account",
    }

    INJECTION_PATTERNS = [
        r"'\s*or\s*'?1'?\s*=\s*'?1'?",  # OR 1=1
        r"sqlite_master",  # metadata access
        r"pragma\s+",      # pragma statements
        r"\.\.\/",         # path traversal
        r";\s*(?:drop|delete|update|insert|create|alter|truncate|replace)",  # stacked queries
        r"union\s+select",  # union select
        r"--\s*\w",        # SQL comments with code
        r"\/\*",           # block comments start
        r"\[\w+\]",        # SQL Server brackets
        r"dbo\.",          # SQL Server schema
        r"\b(?:from|join)\s+[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b",  # cross-db/table qualification
        r"%00",            # null byte injection
        r"replace\s+into",  # REPLACE INTO statement
    ]

    @staticmethod
    def _extract_keywords(text: str) -> set[str]:
        """Extract meaningful keywords from text (lowercase, alphanumeric)."""
        stop_words = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
            "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
            "have", "has", "do", "does", "did", "will", "would", "should", "could",
            "can", "may", "might", "must", "shall", "what", "which", "who", "when",
            "where", "why", "how", "show", "get", "display", "list", "find", "tell",
            "give", "provide", "return", "select", "query", "please", "record",
            "records", "data", "respondent", "respondents", "me", "my", "your", "my",
        }

        words = re.findall(r'\b[a-z_]+\b', text.lower())
        keywords = {w for w in words if len(w) > 2 and w not in stop_words}
        return keywords

    @staticmethod
    def _contains_keyword_reference(sql: str, keywords: set[str]) -> bool:
        sql_lower = sql.lower()
        for kw in keywords:
            if kw in sql_lower:
                return True
        return False

    @staticmethod
    def _extract_from_clause_table(sql: str) -> Optional[str]:
        match = re.search(r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', sql, re.IGNORECASE)
        if match:
            return match.group(1).lower()
        return None

    @staticmethod
    def _extract_select_columns(sql: str) -> list[str]:
        sql_no_comments = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
        
        match = re.search(r'select\s+(.*?)\s+from', sql_no_comments, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        
        select_part = match.group(1)
        select_part = re.sub(r"'(?:''|[^'])*'", "''", select_part)
        select_part = re.sub(r'\s+AS\s+\w+', '', select_part, flags=re.IGNORECASE)
        identifiers = re.findall(r'([a-z_][a-z0-9_]*)', select_part, re.IGNORECASE)
        functions = {"avg", "sum", "count", "min", "max", "distinct", "as", "cast", "case", 
                    "when", "then", "else", "end", "dense_rank", "rank", "row_number", "group_concat"}
        return [i.lower() for i in identifiers if i.lower() not in functions]

    @classmethod
    def validate_semantic_match(
        cls, 
        question: str, 
        sql: Optional[str],
        schema_columns: Optional[set[str]] = None,
    ) -> tuple[bool, Optional[str]]:
        if sql is None:
            return False, "No SQL generated"

        if schema_columns is None:
            schema_columns = cls.SCHEMA_KEYWORDS

        q_keywords = cls._extract_keywords(question)
        q_lower = question.lower()
        out_domain = q_keywords & cls.OUT_OF_DOMAIN_KEYWORDS
        if out_domain and len(out_domain) > 0:
            return False, f"Question contains out-of-domain concepts: {', '.join(out_domain)}"

        for pattern in cls.INJECTION_PATTERNS:
            if re.search(pattern, q_lower, re.IGNORECASE):
                return False, f"Question contains suspicious SQL patterns"

        select_words_match = re.search(r'select\s+(.*?)\s+from', q_lower, re.IGNORECASE)
        if select_words_match:
            select_part = select_words_match.group(1)
            requested_cols = [c.strip() for c in select_part.split(',')]
            requested_cols = [c for c in requested_cols if c and len(c) > 0]
            
            for col in requested_cols:
                col_name = col.split()[0].lower()
                protected = {"oid", "rid", "ctid", "xmin", "xmax", "cmin", "cmax", "password", 
                           "ssn", "credit_card", "pin", "secret", "token", "api_key", "private_key"}
                if col_name in protected:
                    return False, f"Question requests protected/system columns: {col_name}"

        table = cls._extract_from_clause_table(sql)
        if table and table not in {"gaming_mental_health", "mental_health"}:
            return False, f"SQL references unexpected table: {table}"

        select_columns = cls._extract_select_columns(sql)
        sql_lower = sql.lower()
        
        valid_cols = schema_columns | {"count", "avg", "sum", "min", "max", "dense_rank", 
                                        "rank", "row_number", "cast", "case", "when", 
                                        "then", "else", "distinct", "all", "group_concat"}
        
        all_sql_identifiers = set(re.findall(r'\b([a-z_][a-z0-9_]*)\b', sql_lower))
        
        dangerous_cols = {"password", "ssn", "credit_card", "pin", "secret", "token", 
                         "api_key", "private_key", "salary", "account", "users"}
        used_dangerous = all_sql_identifiers & dangerous_cols
        if used_dangerous:
            return False, f"SQL references protected columns: {', '.join(used_dangerous)}"
        
        for col in select_columns:
            if col not in valid_cols and col not in schema_columns:
                if col not in sql_lower.split():
                    return False, f"Unknown column referenced: {col}"

        return True, None
