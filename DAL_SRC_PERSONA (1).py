#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Union, Optional, Any, Tuple
import sqlparse
import traceback


class DataJoiner:
    def __init__(self, db_engine_func):
        self.get_db_engine = db_engine_func

    # =========================================================================
    # validate_rule_logic
    # =========================================================================
    def validate_rule_logic(
        self,
        tables_to_join,
        joins=None,
        filters=None,
        select_columns=None,
        aggregations=None,
        groupby_columns=None,
        orderby_columns=None,
        conditional_aggregations=None,
        having_conditions=None,
        raw_where: Optional[str] = None,
        is_distinct: bool = False
    ):
        try:
            if not tables_to_join:
                return {"valid": False, "error": "No tables_to_join provided."}

            # --- GROUP BY validation (moved above return — Bug fix from v1) ---
            if (aggregations or conditional_aggregations) and select_columns and groupby_columns:
                group_set = {
                    col
                    for cols in groupby_columns.values()
                    for col in cols
                }
                for table, cols in select_columns.items():
                    for source_col in cols.keys():
                        if source_col not in group_set:
                            return {
                                "valid": False,
                                "error": f"Column '{source_col}' must appear in GROUP BY"
                            }

            if len(tables_to_join) == 1:
                table         = tables_to_join[0]
                table_filters = (filters or {}).get(table, {})
                source_cols   = None
                if select_columns and isinstance(select_columns, dict):
                    table_select = select_columns.get(table, {})
                    if isinstance(table_select, dict):
                        source_cols = list(table_select.keys())
                df, _ = self.get_table_data(
                    table_name=table,
                    filters=table_filters,
                    select_columns=source_cols,
                    return_sql=True
                )
            else:
                df, _ = self.universal_data_join(
                    tables_to_join=tables_to_join,
                    joins=joins,
                    filters=filters,
                    select_columns=select_columns,
                    aggregations=aggregations,
                    groupby_columns=groupby_columns,
                    orderby_columns=orderby_columns,
                    conditional_aggregations=conditional_aggregations,
                    having_conditions=having_conditions,
                    raw_where=raw_where,
                    is_distinct=is_distinct,
                    return_sql=True
                )

            return {"valid": True, "preview_rows": df.head(5).to_dict(orient="records")}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    # =========================================================================
    # get_table_data
    # =========================================================================
    def get_table_data(
        self,
        table_name: str,
        filters: Dict[str, Any] = None,
        date_fields: List[str] = None,
        min_max_fields: Dict[str, Dict[str, Union[str, datetime, float]]] = None,
        select_columns: Union[str, List[str], None] = None,
        df: Optional[pd.DataFrame] = None,
        return_sql: bool = True
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, str]]:

        filters        = filters or {}
        date_fields    = date_fields or []
        min_max_fields = min_max_fields or {}

        def parse_value(val, is_date=False):
            if is_date and isinstance(val, str):
                try:
                    return datetime.strptime(val, '%Y-%m-%d')
                except Exception:
                    return datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            return val

        # ── In-memory DataFrame branch ─────────────────────────────────────
        if df is not None:
            df_copy           = df.copy()
            available_columns = df_copy.columns.tolist()

            for field, value in filters.items():
                if field not in available_columns:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Column '{field}' not found in table '{table_name}'"
                    )
                is_date = field in date_fields
                if isinstance(value, (list, tuple)):
                    df_copy = df_copy[df_copy[field].isin([parse_value(v, is_date) for v in value])]
                elif isinstance(value, dict):
                    for op, val in value.items():
                        parsed_val = parse_value(val, is_date)
                        if op == "gt":
                            df_copy = df_copy[df_copy[field] > parsed_val]
                        elif op == "lt":
                            df_copy = df_copy[df_copy[field] < parsed_val]
                        elif op == "ge":
                            df_copy = df_copy[df_copy[field] >= parsed_val]
                        elif op == "le":
                            df_copy = df_copy[df_copy[field] <= parsed_val]
                        elif op == "eq":
                            df_copy = df_copy[df_copy[field] == parsed_val]
                        elif op == "ne":
                            df_copy = df_copy[df_copy[field] != parsed_val]
                        elif op == "in":
                            if not isinstance(parsed_val, (list, tuple)):
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Operator 'in' requires a list for field '{field}'"
                                )
                            df_copy = df_copy[df_copy[field].isin(parsed_val)]
                        elif op.endswith("_dynamic"):
                            try:
                                amount, unit = val.split()
                                amount = int(amount)
                                unit   = unit.lower()
                                if unit.startswith("day"):
                                    delta = timedelta(days=amount)
                                elif unit.startswith("hour"):
                                    delta = timedelta(hours=amount)
                                elif unit.startswith("min"):
                                    delta = timedelta(minutes=amount)
                                else:
                                    raise HTTPException(
                                        status_code=400,
                                        detail=f"Invalid dynamic interval unit: {unit}"
                                    )
                                dynamic_value = datetime.now() - delta
                            except Exception:
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Invalid dynamic value: {val}"
                                )
                            if op.startswith("lt"):
                                df_copy = df_copy[df_copy[field] < dynamic_value]
                            elif op.startswith("gt"):
                                df_copy = df_copy[df_copy[field] > dynamic_value]
                            elif op.startswith("eq"):
                                df_copy = df_copy[df_copy[field] == dynamic_value]
                            else:
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Unsupported dynamic operator '{op}'"
                                )
                        else:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Unsupported operator '{op}' for field '{field}'"
                            )
                else:
                    df_copy = df_copy[df_copy[field] == parse_value(value, is_date)]

            for field, bounds in min_max_fields.items():
                if field not in available_columns:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Column '{field}' not found in table '{table_name}'"
                    )
                if 'min' in bounds:
                    df_copy = df_copy[
                        df_copy[field] >= parse_value(bounds['min'], field in date_fields)
                    ]
                if 'max' in bounds:
                    df_copy = df_copy[
                        df_copy[field] <= parse_value(bounds['max'], field in date_fields)
                    ]

            if select_columns:
                if isinstance(select_columns, str):
                    select_columns = [select_columns]
                missing = [col for col in select_columns if col not in available_columns]
                if missing:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Select columns not found: {missing}"
                    )
                df_copy = df_copy[select_columns]

            return (
                (df_copy, "-- Data filtered from preloaded DataFrame")
                if return_sql else df_copy
            )

        # ── SQL branch ─────────────────────────────────────────────────────
        engine = self.get_db_engine()

        # BUG 8 FIX: set a safe default before the if-block so column_str is
        # always defined regardless of select_columns type.
        column_str = "*"
        if isinstance(select_columns, dict):
            column_parts = []
            for table, col_config in select_columns.items():
                for key, value in col_config.items():
                    if isinstance(value, dict) and "expression" in value:
                        expr  = value["expression"]
                        alias = value.get("alias", key)
                        column_parts.append(f"{expr} AS {alias}")
                    else:
                        src   = value if isinstance(value, str) else key
                        alias = src   if isinstance(value, str) else value
                        if alias and alias != src:
                            column_parts.append(f"{table}.{src} AS {alias}")
                        else:
                            column_parts.append(f"{table}.{src}")
            column_str = ", ".join(column_parts) if column_parts else "*"
        elif isinstance(select_columns, list) and select_columns:
            column_str = ", ".join(select_columns)
        elif isinstance(select_columns, str) and select_columns:
            column_str = select_columns

        query         = f"SELECT {column_str} FROM {table_name}"
        where_clauses = []
        params        = {}
        param_counter = 0

        def new_pname(base):
            nonlocal param_counter
            pname = f"{base}_{param_counter}"
            param_counter += 1
            return pname

        def parse_val_local(val, is_date=False):
            return parse_value(val, is_date)

        for field, value in filters.items():
            is_date = field in date_fields

            if isinstance(value, dict):
                sub_clauses = []
                for op, val in value.items():
                    pname  = new_pname(field)
                    parsed = parse_val_local(val, is_date)

                    if op == "eq":
                        if parsed is None:
                            sub_clauses.append(f"{field} IS NULL")
                        else:
                            sub_clauses.append(f"{field} = %({pname})s")
                            params[pname] = parsed
                    elif op in ("ne", "neq"):
                        if parsed is None:
                            sub_clauses.append(f"{field} IS NOT NULL")
                        else:
                            sub_clauses.append(f"{field} != %({pname})s")
                            params[pname] = parsed
                    elif op == "gt":
                        sub_clauses.append(f"{field} > %({pname})s")
                        params[pname] = parsed
                    elif op == "lt":
                        sub_clauses.append(f"{field} < %({pname})s")
                        params[pname] = parsed
                    elif op == "ge":
                        sub_clauses.append(f"{field} >= %({pname})s")
                        params[pname] = parsed
                    elif op == "le":
                        sub_clauses.append(f"{field} <= %({pname})s")
                        params[pname] = parsed
                    elif op == "in":
                        if not isinstance(val, (list, tuple)):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Operator 'in' requires a list for field '{field}'"
                            )
                        placeholders = []
                        for single in val:
                            pname_i = new_pname(field)
                            placeholders.append(f"%({pname_i})s")
                            params[pname_i] = parse_val_local(single, is_date)
                        sub_clauses.append(f"{field} IN ({', '.join(placeholders)})")
                    elif op == "like":
                        sub_clauses.append(f"{field} LIKE %({pname})s")
                        params[pname] = parsed
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Unsupported operator '{op}' for field '{field}'"
                        )

                if len(sub_clauses) > 1:
                    where_clauses.append("(" + " AND ".join(sub_clauses) + ")")
                elif sub_clauses:
                    where_clauses.append(sub_clauses[0])

            elif isinstance(value, (list, tuple)):
                placeholders = []
                for val in value:
                    pname = new_pname(field)
                    placeholders.append(f"%({pname})s")
                    params[pname] = parse_val_local(val, is_date)
                where_clauses.append(f"{field} IN ({', '.join(placeholders)})")
            else:
                pname = new_pname(field)
                if value is None:
                    where_clauses.append(f"{field} IS NULL")
                else:
                    where_clauses.append(f"{field} = %({pname})s")
                    params[pname] = parse_val_local(value, is_date)

        for field, bounds in min_max_fields.items():
            if 'min' in bounds:
                where_clauses.append(f"{field} >= %(min_{field})s")
                params[f"min_{field}"] = parse_val_local(
                    bounds['min'], field in date_fields
                )
            if 'max' in bounds:
                where_clauses.append(f"{field} <= %(max_{field})s")
                params[f"max_{field}"] = parse_val_local(
                    bounds['max'], field in date_fields
                )

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        with engine.connect() as conn:
            if params:
                with conn.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    cols = [desc[0] for desc in cursor.description]
                    df   = pd.DataFrame(rows, columns=cols)
            else:
                df = pd.read_sql(query, conn)

        sql_preview = query
        for key, val in params.items():
            if isinstance(val, str):
                val_str = f"'{val}'"
            elif isinstance(val, datetime):
                val_str = f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                val_str = str(val)
            sql_preview = sql_preview.replace(f"%({key})s", val_str)
        sql_preview = sqlparse.format(sql_preview, reindent=True).replace("\n", " ")

        return (df, sql_preview) if return_sql else df

    # =========================================================================
    # _build_select_clause
    # BUG 1 FIX: guard against None/empty select_columns
    # BUG 2 FIX: read correct keys from conditional_aggregations JSON shape
    # BUG 3 FIX: add 'in' operator support for CASE WHEN
    # =========================================================================
    def _build_select_clause(
        self,
        select_columns=None,
        aggregations=None,
        conditional_aggregations=None,
        is_distinct: bool = False
    ) -> str:
        parts  = []
        prefix = "SELECT DISTINCT" if is_distinct else "SELECT"

        # operator mapping shared by both plain and conditional aggregations
        op_map = {
            "eq": "=",
            "ne": "!=",
            "neq": "!=",
            "gt": ">",
            "lt": "<",
            "ge": ">=",
            "le": "<="
        }

        # ── Plain SELECT columns ───────────────────────────────────────────
        # BUG 1 FIX: use (select_columns or {}) so None/empty doesn't crash
        for table, col_config in (select_columns or {}).items():
            for key, value in col_config.items():
                if isinstance(value, dict) and "expression" in value:
                    # Advanced expression: {"expression": "to_char(...)", "alias": "col"}
                    expr  = value["expression"]
                    alias = value.get("alias", key)
                    parts.append(f"{expr} AS {alias}")
                else:
                    # Simple mode: {"source_col": "alias"}
                    src   = value if isinstance(value, str) else key
                    alias = value if isinstance(value, str) else key
                    expr  = f"{table}.{src}"
                    if alias and alias != src:
                        expr += f" AS {alias}"
                    parts.append(expr)

        # ── Simple aggregations ────────────────────────────────────────────
        for tbl, aggmap in (aggregations or {}).items():
            for col, info in aggmap.items():
                if isinstance(info, dict):
                    func  = info.get("function", "MAX").upper()
                    alias = info.get("alias") or f"{func.lower()}_{col}"
                else:
                    func  = str(info).upper()
                    alias = f"{func.lower()}_{col}"
                parts.append(f"{func}({tbl}.{col}) AS {alias}")

        # ── Conditional / CASE-style aggregations ─────────────────────────
        # BUG 2 FIX: read from cond["case"]["when"] not cond["when"]
        #            use cond["aggregate"] not hardcoded MIN
        #            use cond["true_result"]/cond["false_result"] not hardcoded strings
        # BUG 3 FIX: support "in" operator inside CASE WHEN
        for tbl, condlist in (conditional_aggregations or {}).items():
            for cond in condlist:
                col       = cond["column"]
                aggregate = cond.get("aggregate", "MIN").upper()
                alias     = cond.get("alias", f"{col}_agg")

                case_def = cond.get("case", {})
                when_def = case_def.get("when", {})
                operator = when_def.get("operator", "eq")
                when_val = when_def.get("value", "TRUE")
                then_val = case_def.get("then", 1)
                else_val = case_def.get("else", 0)

                # FIX: safely convert Python None → SQL NULL, strings → quoted
                def sql_val(v):
                    if v is None:
                        return "NULL"
                    if isinstance(v, str) and not v.startswith("'"):
                        return f"'{v}'"
                    return str(v)

                # Build CASE condition
                if operator == "in":
                    if isinstance(when_val, (list, tuple)):
                        vals_str = ", ".join([sql_val(v) for v in when_val])
                    else:
                        vals_str = str(when_val)
                    case_condition = f"{tbl}.{col} IN ({vals_str})"
                else:
                    op_str = op_map.get(operator, "=")
                    case_condition = f"{tbl}.{col} {op_str} {sql_val(when_val)}"

                # FIX: use sql_val() for then/else so None → NULL, not "None"
                inner_case = (
                    f"CASE WHEN {case_condition} "
                    f"THEN {sql_val(then_val)} ELSE {sql_val(else_val)} END"
                )

                agg_expr = f"{aggregate}({inner_case})"

                final_compare = cond.get("final_compare")
                if final_compare:
                    fc_op  = op_map.get(final_compare.get("operator", "eq"), "=")
                    fc_val = final_compare["value"]
                    true_r  = cond.get("true_result", "1")
                    false_r = cond.get("false_result", "0")

                    # Auto-quote plain strings not already quoted
                    if (isinstance(true_r, str)
                            and not true_r.startswith("'")
                            and not true_r.lstrip("-").isdigit()):
                        true_r = f"'{true_r}'"
                    if (isinstance(false_r, str)
                            and not false_r.startswith("'")
                            and not false_r.lstrip("-").isdigit()):
                        false_r = f"'{false_r}'"

                    expr = (
                        f"CASE WHEN {agg_expr} {fc_op} {fc_val} "
                        f"THEN {true_r} ELSE {false_r} END"
                    )
                else:
                    expr = agg_expr

                parts.append(f"{expr} AS {alias}")

        if not parts:
            return f"{prefix} *"
        return f"{prefix} " + ", ".join(parts)

    # =========================================================================
    # _build_where_clause
    # BUG 4 FIX: remove undefined new_pname/params references
    # BUG 5 FIX: fix indentation so scalar else branch is reachable;
    #             use correct variable name (condition vs val)
    # =========================================================================
    def _build_where_clause(self, filters: Optional[Dict] = None) -> str:
        if not filters:
            return ""

        conds = []
        for tbl, fieldmap in (filters or {}).items():
            for field, condition in fieldmap.items():
                col_ref = f"{tbl}.{field}"

                if isinstance(condition, dict):
                    for op, v in condition.items():
                        if op == "eq":
                            if v is None:
                                conds.append(f"{col_ref} IS NULL")
                            elif isinstance(v, str):
                                conds.append(f"{col_ref} = '{v}'")
                            else:
                                conds.append(f"{col_ref} = {v}")

                        elif op in ("ne", "neq"):
                            if v is None:
                                conds.append(f"{col_ref} IS NOT NULL")
                            elif isinstance(v, str):
                                conds.append(f"{col_ref} != '{v}'")
                            else:
                                conds.append(f"{col_ref} != {v}")

                        elif op == "gt":
                            conds.append(f"{col_ref} > {v}")
                        elif op == "lt":
                            conds.append(f"{col_ref} < {v}")
                        elif op == "ge":
                            conds.append(f"{col_ref} >= {v}")
                        elif op == "le":
                            conds.append(f"{col_ref} <= {v}")

                        elif op == "in":
                            # BUG 4 FIX: use 'v' (the actual list), not 'val'/'condition'
                            if not isinstance(v, (list, tuple)):
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Operator 'in' requires a list for field '{field}'"
                                )
                            vals = ", ".join(
                                [f"'{i}'" if isinstance(i, str) else str(i) for i in v]
                            )
                            conds.append(f"{col_ref} IN ({vals})")

                        elif op == "like":
                            conds.append(f"{col_ref} LIKE '{v}'")

                        else:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Unsupported operator '{op}' for field '{field}'"
                            )

                # BUG 5 FIX: this else is correctly indented under
                # "if isinstance(condition, dict)", uses 'condition' not 'val'
                else:
                    if condition is None:
                        conds.append(f"{col_ref} IS NULL")
                    elif isinstance(condition, str):
                        conds.append(f"{col_ref} = '{condition}'")
                    else:
                        conds.append(f"{col_ref} = {condition}")

        return "WHERE " + " AND ".join(conds) if conds else ""

    # =========================================================================
    # _build_group_by_clause  (unchanged, kept for completeness)
    # =========================================================================
    def _build_group_by_clause(self, groupby_columns: Optional[Dict] = None) -> str:
        if not groupby_columns:
            return ""
        parts = []
        for tbl, cols in groupby_columns.items():
            for col in cols:
                parts.append(f"{tbl}.{col}")
        return "GROUP BY " + ", ".join(parts) if parts else ""

    # =========================================================================
    # _build_having_clause  (unchanged, kept for completeness)
    # =========================================================================
    def _build_having_clause(self, having_conditions: Optional[List[Dict]] = None) -> str:
        if not having_conditions:
            return ""
        cond_strs = []
        for c in having_conditions:
            expr    = c.get("expression", "")
            op      = c.get("operator", "=")
            val     = c.get("value")
            logical = c.get("logical") or ""
            cond    = f"{expr} {op} {val}"
            if logical:
                cond = f"{logical} {cond}"
            cond_strs.append(cond)
        return "HAVING " + " ".join(cond_strs) if cond_strs else ""

    # =========================================================================
    # _build_subquery
    # BUG 7 FIX: pass explicit empty dicts so _build_select_clause never
    #             receives None for select_columns
    # =========================================================================
    def _build_subquery(self, sub_def: dict) -> str:
        select_clause = self._build_select_clause(
            select_columns=sub_def.get("select_columns") or {},        # BUG 7 FIX
            aggregations=sub_def.get("aggregations") or {},
            conditional_aggregations=sub_def.get("conditional_aggregations") or {},
            is_distinct=sub_def.get("is_distinct", False)
        )

        from_joins = self._build_from_and_joins(
            tables_to_join=sub_def.get("tables_to_join", []),
            joins=sub_def.get("joins", [])
        )

        where_clause    = self._build_where_clause(sub_def.get("filters"))
        group_by_clause = self._build_group_by_clause(sub_def.get("groupby_columns"))
        having_clause   = self._build_having_clause(sub_def.get("having_conditions"))

        sql = f"{select_clause}\n{from_joins}"
        if where_clause:    sql += f"\n{where_clause}"
        if group_by_clause: sql += f"\n{group_by_clause}"
        if having_clause:   sql += f"\n{having_clause}"

        return sql.strip()

    # =========================================================================
    # _build_from_and_joins  (unchanged logic, kept for completeness)
    # =========================================================================
    def _build_from_and_joins(
        self,
        tables_to_join: List[str],
        joins: Optional[List[Dict]] = None,
        base_table: Optional[str] = None
    ) -> str:
        if not base_table and tables_to_join:
            base_table = tables_to_join[0]

        clauses = [f"FROM {base_table}"]

        for j in joins or []:
            jt = j.get("join_type", "INNER").upper()
            lt = j["left_table"]

            if "right_subquery" in j and isinstance(j["right_subquery"], dict):
                sub_sql = self._build_subquery(j["right_subquery"])
                alias   = j["right_subquery"].get("alias", "subq")
                on_conds = " AND ".join(
                    f"{lt}.{lk} = {alias}.{rk}"
                    for lk, rk in zip(
                        j.get("left_keys", []),
                        j.get("right_keys", [])
                    )
                ) or "TRUE"
                clauses.append(f"{jt} JOIN ({sub_sql}) AS {alias} ON {on_conds}")
            else:
                rt = j["right_table"]
                on_conds = " AND ".join(
                    f"{lt}.{lk} = {rt}.{rk}"
                    for lk, rk in zip(
                        j.get("left_keys", []),
                        j.get("right_keys", [])
                    )
                ) or "TRUE"
                clauses.append(f"{jt} JOIN {rt} ON {on_conds}")

        return "\n".join(clauses)

    # =========================================================================
    # build_sql_query
    # BUG 6 FIX: ORDER BY supports "_alias" sentinel for alias-based ordering
    # =========================================================================
    def build_sql_query(
        self,
        tables_to_join: List[str],
        joins=None,
        filters=None,
        select_columns=None,
        aggregations=None,
        groupby_columns=None,
        orderby_columns=None,
        conditional_aggregations=None,
        having_conditions=None,
        raw_where: Optional[str] = None,
        is_distinct: bool = False
    ) -> str:

        select_clause = self._build_select_clause(
            select_columns=select_columns or {},
            aggregations=aggregations,
            conditional_aggregations=conditional_aggregations,
            is_distinct=is_distinct
        )

        from_joins = self._build_from_and_joins(
            tables_to_join=tables_to_join,
            joins=joins
        )

        if raw_where:
            where_clause = f"WHERE {raw_where}"
        else:
            where_clause = self._build_where_clause(filters)

        group_by_clause = self._build_group_by_clause(groupby_columns)
        having_clause   = self._build_having_clause(having_conditions)

        # BUG 6 FIX: if table key is "_alias", emit column name only (no table prefix)
        order_parts = []
        for tbl, colmap in (orderby_columns or {}).items():
            for col, dir_ in colmap.items():
                if tbl == "_alias":
                    order_parts.append(f"{col} {dir_.upper()}")
                else:
                    order_parts.append(f"{tbl}.{col} {dir_.upper()}")
        order_clause = "ORDER BY " + ", ".join(order_parts) if order_parts else ""

        query = f"{select_clause}\n{from_joins}"
        if where_clause:    query += f"\n{where_clause}"
        if group_by_clause: query += f"\n{group_by_clause}"
        if having_clause:   query += f"\n{having_clause}"
        if order_clause:    query += f"\n{order_clause}"

        return sqlparse.format(
            query, reindent=True, keyword_case='upper'
        ).replace("\n", " ")

    # =========================================================================
    # universal_data_join  (unchanged signature, kept for completeness)
    # =========================================================================
    def universal_data_join(
        self,
        tables_to_join: List[str],
        joins: List[Dict[str, Any]] = None,
        filters: Dict[str, dict] = None,
        select_columns: Dict[str, Dict[str, str]] = None,
        aggregations: Dict[str, dict] = None,
        groupby_columns: Dict[str, List[str]] = None,
        orderby_columns: Dict[str, dict] = None,
        conditional_aggregations: Dict[str, list] = None,
        having_conditions: List[dict] = None,
        raw_where: Optional[str] = None,
        is_distinct: bool = False,
        return_sql: bool = True
    ):
        query  = self.build_sql_query(
            tables_to_join=tables_to_join,
            joins=joins,
            filters=filters,
            select_columns=select_columns,
            aggregations=aggregations,
            groupby_columns=groupby_columns,
            orderby_columns=orderby_columns,
            conditional_aggregations=conditional_aggregations,
            having_conditions=having_conditions,
            raw_where=raw_where,
            is_distinct=is_distinct
        )
        engine = self.get_db_engine()
        df     = pd.read_sql(query, engine)
        return (df, query) if return_sql else df
        
def get_db_engine():
    return create_engine("postgresql://assetlinkagedbuser:assetdb5454@genw-uatint-aurora-cluster-instance-1.cjss8686maux.ap-south-1.rds.amazonaws.com:5432/assetlinkagedb")


# =============================================================================
# RuleJoiner
# =============================================================================
from sqlalchemy import text, create_engine
import json


class RuleJoiner:

    def __init__(self, db_engine_func):
        self.get_db_engine = db_engine_func
        self.RULE_REGISTRY = {}

    def rule(self, rule_id):
        def decorator(func):
            self.RULE_REGISTRY[rule_id] = func
            return func
        return decorator

    def store_rule_output(self, rule_id, df, sql_preview):
        try:
            json_output = json.loads(df.to_json(orient="records"))
            upsert_sql  = text("""
                INSERT INTO rule_outputs (rule_id, output, sql_preview, updated_at)
                VALUES (:rule_id, :output, :sql_preview, NOW())
                ON CONFLICT (rule_id) DO UPDATE SET
                    output      = EXCLUDED.output,
                    sql_preview = EXCLUDED.sql_preview,
                    updated_at  = NOW();
            """)
            with self.get_db_engine().begin() as conn:
                conn.execute(upsert_sql, {
                    "rule_id":     rule_id,
                    "output":      json.dumps(json_output),
                    "sql_preview": sql_preview
                })
        except Exception as e:
            print(f"Error storing rule output: {e}")
            raise

    def get_cached_rule_output(self, rule_id: str):
        engine = self.get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT output FROM rule_outputs WHERE rule_id = :rid"),
                {"rid": rule_id}
            )
            row = result.mappings().first()
            return json.loads(row["output"]) if row else None

    def run_rule(self, rule_id: str):
        cached = self.get_cached_rule_output(rule_id)
        if cached:
            return cached
        result_df = self.RULE_REGISTRY[rule_id]()
        return json.loads(result_df.to_json(orient='records'))



rulejoiner = RuleJoiner(get_db_engine)


# =============================================================================
# FastAPI App
# =============================================================================
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Union, List, Optional
import uvicorn, nest_asyncio, socket, numpy as np, os
from threading import Thread

nest_asyncio.apply()

app = FastAPI(title="Business Rules Engine")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)



dj= DataJoiner(get_db_engine)
TEMP_RULES = {}


# ── /rules ────────────────────────────────────────────────────────────────────
@app.get("/rules", response_class=PlainTextResponse)
def list_rules():
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT rule_id, description FROM rule_definitions ORDER BY rule_id")
        ).mappings().all()

    lines = ["📋 Business Rules:\n"]
    for row in rows:
        lines.append(f"🔹 {row['rule_id']}: {row['description']}")

    return "\n".join(lines)


# ── /run_rule/{rule_id} ───────────────────────────────────────────────────────
@app.get("/run_rule/{rule_id}")
def run_rule_by_id(rule_id: str):
    engine = get_db_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM rule_definitions WHERE rule_id = :rid"),
            {"rid": rule_id}
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")

    try:
        row_map = row._mapping
    except AttributeError:
        row_map = dict(row)

    def parse_jsonb_field(field, default=None):
        """Safely parse a JSONB field that may arrive as str, dict, list, or None."""
        if default is None:
            default = {}
        if field is None:
            return default
        if isinstance(field, tuple) and len(field) == 1:
            field = field[0]
        if isinstance(field, str):
            stripped = field.strip()
            if stripped in ("", "null", "None"):
                return default
            return json.loads(stripped)
        if isinstance(field, (dict, list)):
            return field
        return default

    try:
        tables_to_join           = parse_jsonb_field(row_map.get("tables_to_join"), default=[])
        joins                    = parse_jsonb_field(row_map.get("joins"),           default=[])
        filters                  = parse_jsonb_field(row_map.get("filters"))
        aggregations             = parse_jsonb_field(row_map.get("aggregations"))
        groupby_columns          = parse_jsonb_field(row_map.get("groupby_columns"))
        orderby_columns          = parse_jsonb_field(row_map.get("orderby_columns"))
        select_columns           = parse_jsonb_field(row_map.get("select_columns"))
        conditional_aggregations = parse_jsonb_field(row_map.get("conditional_aggregations"))
        having_conditions        = parse_jsonb_field(row_map.get("having_conditions"), default=[])
        raw_where                = row_map.get("raw_where")
        is_distinct              = row_map.get("is_distinct", False)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error parsing JSONB fields: {repr(e)}")

    try:
        datajoiner = DataJoiner(get_db_engine)

        if len(tables_to_join) == 1:
            df, sql_preview = datajoiner.get_table_data(
                table_name=tables_to_join[0],
                filters=filters,
                select_columns=select_columns,
                return_sql=True
            )
        else:
            df, sql_preview = datajoiner.universal_data_join(
                tables_to_join=tables_to_join,
                joins=joins,
                filters=filters,
                select_columns=select_columns,
                aggregations=aggregations,
                groupby_columns=groupby_columns,
                orderby_columns=orderby_columns,
                conditional_aggregations=conditional_aggregations,
                having_conditions=having_conditions,
                raw_where=raw_where,
                is_distinct=is_distinct,
                return_sql=True
            )

        result_json = json.loads(df.replace({np.nan: None}).to_json(orient="records"))
        rulejoiner.store_rule_output(rule_id, df, sql_preview)

        return {
            "rule_id":          rule_id,
            "description":      row_map.get("description", ""),
            "long_description": row_map.get("long_description", ""),
            "row_count":        len(df),
            "sql_preview":      sql_preview,
            "result":           result_json
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error running rule: {repr(e)}")


# ── Pydantic models ───────────────────────────────────────────────────────────

# BUG 9 FIX: right_table is Optional so subquery joins (right_subquery) don't fail
class JoinDefinition(BaseModel):
    left_table:     str
    right_table:    Optional[str]            = None   # optional for subquery joins
    right_subquery: Optional[Dict[str, Any]] = None   # support nested subquery joins
    left_keys:      List[str]
    right_keys:     List[str]
    join_type:      Optional[str]            = "left"


class RuleCreateModel(BaseModel):
    rule_id:                  str
    description:              str
    tables_to_join:           List[str]
    joins:                    List[JoinDefinition]
    filters:                  Optional[Dict[str, Any]]                  = {}
    select_columns:           Optional[Dict[str, Dict[str, Any]]]       = {}
    aggregations:             Optional[Dict[str, Dict[str, Any]]]       = {}
    groupby_columns:          Optional[Dict[str, List[str]]]            = {}
    orderby_columns:          Optional[Dict[str, Dict[str, str]]]       = {}
    conditional_aggregations: Optional[Dict[str, List[Dict[str, Any]]]] = {}
    having_conditions:        Optional[List[Dict[str, Any]]]            = []
    long_description:         Optional[str]                             = None
    raw_where:                Optional[str]                             = None
    is_distinct:              Optional[bool]                            = False


# ── /add_rule ─────────────────────────────────────────────────────────────────
@app.post("/add_rule")
def add_rule(rule: RuleCreateModel):
    engine = get_db_engine()

    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT 1 FROM rule_definitions WHERE rule_id = :rid"),
            {"rid": rule.rule_id}
        ).mappings().first()
        if existing:
            raise HTTPException(status_code=400, detail="Rule ID already exists")

    validation = dj.validate_rule_logic(
        tables_to_join=rule.tables_to_join,
        joins=[j.dict() for j in rule.joins],
        filters=rule.filters,
        select_columns=rule.select_columns,
        aggregations=rule.aggregations,
        groupby_columns=rule.groupby_columns,
        orderby_columns=rule.orderby_columns,
        conditional_aggregations=rule.conditional_aggregations,
        having_conditions=rule.having_conditions,
        raw_where=rule.raw_where,
        is_distinct=rule.is_distinct
    )
    if not validation["valid"]:
        raise HTTPException(status_code=400, detail=validation["error"])

    insert_sql = text("""
        INSERT INTO rule_definitions (
            rule_id, description, tables_to_join, joins, filters,
            select_columns, aggregations, groupby_columns, orderby_columns,
            conditional_aggregations, having_conditions, long_description,
            raw_where, is_distinct
        )
        VALUES (
            :rule_id, :description, :tables_to_join, :joins, :filters,
            :select_columns, :aggregations, :groupby_columns, :orderby_columns,
            :conditional_aggregations, :having_conditions, :long_description,
            :raw_where, :is_distinct
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "rule_id":                  rule.rule_id,
            "description":              rule.description,
            "tables_to_join":           json.dumps(rule.tables_to_join),
            "joins":                    json.dumps([j.dict() for j in rule.joins]),
            "filters":                  json.dumps(rule.filters),
            "select_columns":           json.dumps(rule.select_columns),
            "aggregations":             json.dumps(rule.aggregations),
            "groupby_columns":          json.dumps(rule.groupby_columns),
            "orderby_columns":          json.dumps(rule.orderby_columns),
            "conditional_aggregations": json.dumps(rule.conditional_aggregations),
            "having_conditions":        json.dumps(rule.having_conditions),
            "long_description":         rule.long_description,
            "raw_where":                rule.raw_where,
            "is_distinct":              rule.is_distinct
        })

    return {
        "message": "Rule created successfully",
        "rule_id": rule.rule_id,
        "preview": validation["preview_rows"]
    }


# ── /delete_rule/{rule_id} ────────────────────────────────────────────────────
@app.delete("/delete_rule/{rule_id}")
def delete_rule(rule_id: str):
    engine = get_db_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM rule_definitions WHERE rule_id = :rid"),
            {"rid": rule_id}
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
    return {"message": f"Rule '{rule_id}' deleted successfully"}


# ── /table ────────────────────────────────────────────────────────────────────
@app.post("/table", response_class=JSONResponse)
def get_table_data_view(
    table_name:     str = Form(...),
    filters:        str = Form(""),
    select_columns: str = Form("")
):
    try:
        filters_dict = json.loads(filters) if filters else {}
        columns      = [c.strip() for c in select_columns.split(",")] if select_columns else None
        df, sql      = dj.get_table_data(
            table_name=table_name,
            filters=filters_dict,
            select_columns=columns,
            return_sql=True
        )
        return {"sql_query": sql, "result": df.to_dict(orient="records")}
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}


# ── /query ────────────────────────────────────────────────────────────────────
engine = get_db_engine()

@app.post("/query", response_class=JSONResponse)
def run_sql_query(sql: str = Form(...)):
    try:
        if not sql.strip().lower().startswith("select"):
            return {"error": "Only SELECT queries are allowed"}

        df = pd.read_sql(sql, con=engine)
        return {"sql_query": sql, "result": df.to_dict(orient="records")}

    except Exception as e:
        logger.exception("Error running query")
        return {"error": str(e)}


@app.get("/execute_rule/{rule_id}")
def execute_rule(rule_id: str):
    try:
        return run_rule_by_id(rule_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# ── Server startup ────────────────────────────────────────────────────────────
PORT = 8000

host_ip = socket.gethostbyname(socket.gethostname())
print(f"Fast API Running at http://{host_ip}:8000/docs")

os.makedirs("static",    exist_ok=True)
os.makedirs("templates", exist_ok=True)


# In[ ]:





# In[ ]:




