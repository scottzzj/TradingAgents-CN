"""
基于MongoDB的股票筛选服务
利用本地数据库中的股票基础信息进行高效筛选
"""

import logging
import asyncio
import contextlib
import io
import math
import re
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.core.database import get_mongo_db
# from app.models.screening import ScreeningCondition  # 避免循环导入

logger = logging.getLogger(__name__)


class DatabaseScreeningService:
    """基于数据库的股票筛选服务"""
    
    def __init__(self):
        # 使用视图而不是基础信息表，视图已经包含了实时行情数据
        self.collection_name = "stock_screening_view"
        self._quotes_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
        self._industry_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
        self._roe_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
        self._quotes_cache_ttl = 300
        self._industry_cache_ttl = 6 * 60 * 60
        self._roe_cache_ttl = 6 * 60 * 60
        self._industry_min_usable_count = 1000
        
        # 支持的基础信息字段映射
        self.basic_fields = {
            # 基本信息
            "code": "code",
            "name": "name", 
            "industry": "industry",
            "area": "area",
            "market": "market",
            "list_date": "list_date",
            
            # 市值信息 (亿元)
            "total_mv": "total_mv",      # 总市值
            "circ_mv": "circ_mv",        # 流通市值
            "market_cap": "total_mv",    # 市值别名

            # 财务指标
            "pe": "pe",                  # 市盈率
            "pb": "pb",                  # 市净率
            "pe_ttm": "pe_ttm",         # 滚动市盈率
            "pb_mrq": "pb_mrq",         # 最新市净率
            "roe": "roe",                # 净资产收益率（最近一期）

            # 交易指标
            "turnover_rate": "turnover_rate",  # 换手率%
            "volume_ratio": "volume_ratio",    # 量比

            # 实时行情字段（需要从 market_quotes 关联查询）
            "pct_chg": "pct_chg",              # 涨跌幅%
            "amount": "amount",                # 成交额（万元）
            "close": "close",                  # 收盘价
            "volume": "volume",                # 成交量
        }
        
        # 支持的操作符
        self.operators = {
            ">": "$gt",
            "<": "$lt", 
            ">=": "$gte",
            "<=": "$lte",
            "==": "$eq",
            "!=": "$ne",
            "between": "$between",  # 自定义处理
            "in": "$in",
            "not_in": "$nin",
            "contains": "$regex",   # 字符串包含
        }
    
    async def can_handle_conditions(self, conditions: List[Dict[str, Any]]) -> bool:
        """
        检查是否可以完全通过数据库筛选处理这些条件
        
        Args:
            conditions: 筛选条件列表
            
        Returns:
            bool: 是否可以处理
        """
        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            
            # 检查字段是否支持
            if field not in self.basic_fields:
                logger.debug(f"字段 {field} 不支持数据库筛选")
                return False
            
            # 检查操作符是否支持
            if operator not in self.operators:
                logger.debug(f"操作符 {operator} 不支持数据库筛选")
                return False
        
        return True
    
    async def screen_stocks(
        self,
        conditions: List[Dict[str, Any]],
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        source: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        基于数据库进行股票筛选

        Args:
            conditions: 筛选条件列表
            limit: 返回数量限制
            offset: 偏移量
            order_by: 排序条件 [{"field": "total_mv", "direction": "desc"}]
            source: 数据源（可选），默认使用优先级最高的数据源

        Returns:
            Tuple[List[Dict], int]: (筛选结果, 总数量)
        """
        try:
            db = get_mongo_db()
            collection = db[self.collection_name]
            source_candidates = [source] if source else None

            # 🔥 获取数据源优先级配置
            if not source:
                from app.core.unified_config import UnifiedConfigManager
                config = UnifiedConfigManager()
                data_source_configs = await config.get_data_source_configs_async()

                logger.info(f"🔍 [database_screening] 获取到 {len(data_source_configs)} 个数据源配置")
                for ds in data_source_configs:
                    logger.info(f"   - {ds.name}: type={ds.type}, priority={ds.priority}, enabled={ds.enabled}")

                # 提取启用的数据源，按优先级排序
                enabled_sources = [
                    ds.type.lower() for ds in data_source_configs
                    if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
                ]

                logger.info(f"🔍 [database_screening] 启用的数据源（按优先级）: {enabled_sources}")

                if not enabled_sources:
                    enabled_sources = ['tushare', 'akshare', 'baostock']
                    logger.warning(f"⚠️ [database_screening] 没有启用的数据源，使用默认: {enabled_sources}")

                source_candidates = enabled_sources or ['tushare', 'akshare', 'baostock']
                logger.info(f"✅ [database_screening] 数据源候选: {source_candidates}")

            # 构建查询条件（现在视图已包含实时行情数据，可以直接查询所有字段）
            base_query = await self._build_query(conditions)

            # 构建排序条件
            sort_conditions = self._build_sort_conditions(order_by)

            source_candidates = source_candidates or ['tushare', 'akshare', 'baostock']
            results = []
            codes = []
            total_count = 0
            selected_source = None

            for candidate in source_candidates:
                query = dict(base_query)
                query["source"] = candidate
                logger.info(f"📋 数据库查询条件: {query}")

                total_count = await collection.count_documents(query)
                if total_count == 0 and source is None:
                    logger.info(f"⚠️ [database_screening] 数据源 {candidate} 无结果，尝试下一个数据源")
                    continue

                cursor = collection.find(query)
                if sort_conditions:
                    cursor = cursor.sort(sort_conditions)
                cursor = cursor.skip(offset).limit(limit)

                results = []
                codes = []
                async for doc in cursor:
                    result = self._format_result(doc)
                    results.append(result)
                    codes.append(doc.get("code"))

                selected_source = candidate
                break

            if codes:
                await self._enrich_with_financial_data(results, codes)

            if total_count == 0 and conditions and self._can_use_external_fallback(conditions, order_by):
                logger.info("🔁 数据库筛选无结果，尝试使用外部实时数据兜底筛选")
                fallback_results, fallback_total = await self._screen_with_external_fallback(
                    conditions=conditions,
                    limit=limit,
                    offset=offset,
                    order_by=order_by,
                    source_candidates=source_candidates,
                )
                if fallback_total > 0:
                    logger.info(f"✅ 外部实时数据兜底筛选完成: 总数={fallback_total}, 返回={len(fallback_results)}")
                    return fallback_results, fallback_total

            logger.info(f"✅ 数据库筛选完成: 总数={total_count}, 返回={len(results)}, 数据源={selected_source}")

            return results, total_count
            
        except Exception as e:
            logger.error(f"❌ 数据库筛选失败: {e}")
            raise Exception(f"数据库筛选失败: {str(e)}")

    def _condition_field(self, condition: Any) -> Optional[str]:
        return condition.get("field") if isinstance(condition, dict) else getattr(condition, "field", None)

    def _condition_operator(self, condition: Any) -> Optional[str]:
        op = condition.get("operator") if isinstance(condition, dict) else getattr(condition, "operator", None)
        return str(op) if op is not None else None

    def _condition_value(self, condition: Any) -> Any:
        return condition.get("value") if isinstance(condition, dict) else getattr(condition, "value", None)

    def _can_use_external_fallback(
        self,
        conditions: List[Dict[str, Any]],
        order_by: Optional[List[Dict[str, str]]]
    ) -> bool:
        """外部兜底只处理页面当前暴露的低成本筛选字段。"""
        supported_fields = {
            "industry", "total_mv", "market_cap", "circ_mv",
            "pe", "pe_ttm", "pb", "pb_mrq", "roe",
            "pct_chg", "amount", "close", "volume",
        }
        for condition in conditions:
            field = self._condition_field(condition)
            if field not in supported_fields:
                return False
        for order in order_by or []:
            field = order.get("field")
            mapped = self.basic_fields.get(field, field)
            if mapped not in supported_fields:
                return False
        return True

    async def _screen_with_external_fallback(
        self,
        conditions: List[Dict[str, Any]],
        limit: int,
        offset: int,
        order_by: Optional[List[Dict[str, str]]],
        source_candidates: List[str],
    ) -> Tuple[List[Dict[str, Any]], int]:
        db = get_mongo_db()
        base_docs = await self._load_base_stock_docs(db, source_candidates)
        if not base_docs:
            return [], 0

        fields = {self.basic_fields.get(self._condition_field(c), self._condition_field(c)) for c in conditions}
        sort_fields = {self.basic_fields.get(o.get("field"), o.get("field")) for o in (order_by or [])}
        needs_quotes = bool((fields | sort_fields) & {
            "total_mv", "circ_mv", "pe", "pe_ttm", "pb", "pb_mrq",
            "pct_chg", "amount", "close", "volume",
        })
        # 前端默认按市值排序；兜底路径也拉行情，保证排序和展示字段可用。
        needs_quotes = needs_quotes or bool(order_by)
        needs_industry = "industry" in fields
        needs_roe = "roe" in fields or "roe" in sort_fields

        codes = [doc.get("code") for doc in base_docs if doc.get("code")]
        quotes_map = await self._get_tencent_quotes(codes) if needs_quotes else {}
        industry_map = await self._get_baostock_industry_map() if needs_industry else {}
        roe_map = await self._get_akshare_roe_map() if needs_roe else {}

        rows = []
        for doc in base_docs:
            row = self._merge_external_row(doc, quotes_map, industry_map, roe_map)
            if self._matches_conditions(row, conditions):
                rows.append(row)

        rows = self._sort_external_rows(rows, order_by)
        total = len(rows)
        return rows[offset:offset + limit], total

    async def _load_base_stock_docs(self, db: Any, source_candidates: List[str]) -> List[Dict[str, Any]]:
        projection = {
            "_id": 0, "code": 1, "symbol": 1, "name": 1, "industry": 1,
            "area": 1, "market": 1, "list_date": 1, "source": 1,
        }
        for candidate in source_candidates or ["akshare", "tushare", "baostock"]:
            docs = await db["stock_basic_info"].find({"source": candidate}, projection).to_list(length=None)
            if docs:
                return docs
        return await db["stock_basic_info"].find({}, projection).to_list(length=None)

    def _merge_external_row(
        self,
        doc: Dict[str, Any],
        quotes_map: Dict[str, Dict[str, Any]],
        industry_map: Dict[str, str],
        roe_map: Dict[str, float],
    ) -> Dict[str, Any]:
        code = str(doc.get("code") or doc.get("symbol") or "").zfill(6)
        row = self._format_result(doc)
        row["code"] = code
        row["symbol"] = code
        if not row.get("name"):
            row["name"] = doc.get("name") or code

        industry = doc.get("industry") or industry_map.get(code)
        if industry:
            row["industry"] = industry

        quote = quotes_map.get(code)
        if quote:
            row.update({k: v for k, v in quote.items() if v is not None})

        if code in roe_map:
            row["roe"] = roe_map[code]

        row["source"] = row.get("source") or "external_fallback"
        return row

    async def _get_tencent_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        cached = self._quotes_cache.get("data")
        if cached is not None and time.time() - self._quotes_cache.get("ts", 0) < self._quotes_cache_ttl:
            return cached

        data = await asyncio.to_thread(self._fetch_tencent_quotes, codes)
        self._quotes_cache = {"ts": time.time(), "data": data}
        return data

    def _fetch_tencent_quotes(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        def prefixed(code: str) -> str:
            code6 = str(code).zfill(6)
            if code6.startswith(("6", "9")):
                return f"sh{code6}"
            if code6.startswith(("8", "4")):
                return f"bj{code6}"
            return f"sz{code6}"

        result: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(codes), 800):
            batch = [c for c in codes[i:i + 800] if c]
            if not batch:
                continue
            url = "https://qt.gtimg.cn/q=" + ",".join(prefixed(c) for c in batch)
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                raw = urllib.request.urlopen(req, timeout=15).read().decode("gbk", "ignore")
            except Exception as e:
                logger.warning(f"腾讯行情批次拉取失败: {e}")
                continue

            for line in raw.strip().split(";"):
                if not line.strip() or '="' not in line:
                    continue
                try:
                    key = line.split("=")[0].split("_")[-1]
                    vals = line.split('"')[1].split("~")
                    if len(vals) < 53:
                        continue
                    code = key[2:].zfill(6)
                    pe_ttm = self._safe_float(vals[39])
                    pb = self._safe_float(vals[46])
                    pe_static = self._safe_float(vals[52])
                    result[code] = {
                        "name": vals[1] or None,
                        "close": self._safe_float(vals[3]),
                        "pct_chg": self._safe_float(vals[32]),
                        "high": self._safe_float(vals[33]),
                        "low": self._safe_float(vals[34]),
                        "volume": self._safe_float(vals[36]),
                        "amount": self._safe_float(vals[37]),  # 万元
                        "turnover_rate": self._safe_float(vals[38]),
                        "pe": pe_static if pe_static is not None else pe_ttm,
                        "pe_ttm": pe_ttm,
                        "total_mv": self._safe_float(vals[44]),  # 亿元
                        "circ_mv": self._safe_float(vals[45]),   # 亿元
                        "pb": pb,
                        "pb_mrq": pb,
                        "volume_ratio": self._safe_float(vals[49]),
                    }
                except Exception:
                    continue
        logger.info(f"✅ 腾讯行情兜底获取 {len(result)} 条")
        return result

    async def _get_baostock_industry_map(self) -> Dict[str, str]:
        cached = self._industry_cache.get("data")
        if cached is not None and time.time() - self._industry_cache.get("ts", 0) < self._industry_cache_ttl:
            return cached

        data = await asyncio.to_thread(self._fetch_baostock_industry_map)
        if len(data) < self._industry_min_usable_count:
            logger.warning(f"BaoStock 行业数据不完整（{len(data)} 条），改用 AKShare 行业兜底")
            akshare_data = await asyncio.to_thread(self._fetch_akshare_industry_map)
            if len(akshare_data) > len(data):
                data = akshare_data

        # 不缓存空或明显不完整的行业表，避免一次临时失败导致页面下拉长期异常。
        if len(data) >= self._industry_min_usable_count:
            self._industry_cache = {"ts": time.time(), "data": data}
        return data

    def _fetch_baostock_industry_map(self) -> Dict[str, str]:
        try:
            import baostock as bs
        except ImportError:
            logger.warning("BaoStock 未安装，无法获取行业兜底数据")
            return {}

        industry_map: Dict[str, str] = {}
        lg = bs.login()
        if getattr(lg, "error_code", "") != "0":
            logger.warning(f"BaoStock 登录失败，无法获取行业: {getattr(lg, 'error_msg', '')}")
            return {}
        try:
            rs = bs.query_stock_industry()
            while rs.error_code == "0" and rs.next():
                row = rs.get_row_data()
                code = str(row[1] if len(row) > 1 else "").replace("sh.", "").replace("sz.", "").zfill(6)
                raw_industry = row[3] if len(row) > 3 else ""
                industry = re.sub(r"^[A-Z]\d+", "", str(raw_industry)).strip() if raw_industry else ""
                if code and industry:
                    industry_map[code] = industry
        finally:
            bs.logout()

        logger.info(f"✅ BaoStock 行业兜底获取 {len(industry_map)} 条")
        return industry_map

    def _fetch_akshare_industry_map(self) -> Dict[str, str]:
        try:
            import akshare as ak
        except ImportError:
            logger.warning("AKShare 未安装，无法获取行业兜底数据")
            return {}

        for report_date in self._financial_report_date_candidates():
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = ak.stock_yjbb_em(date=report_date)
                if df is None or getattr(df, "empty", True):
                    continue

                code_col = "股票代码" if "股票代码" in df.columns else None
                industry_col = "所处行业" if "所处行业" in df.columns else None
                if not code_col or not industry_col:
                    continue

                industry_map: Dict[str, str] = {}
                for _, row in df.iterrows():
                    code = str(row.get(code_col) or "").zfill(6)
                    industry = self._safe_text(row.get(industry_col))
                    if code and industry:
                        industry_map[code] = industry
                if industry_map:
                    logger.info(f"✅ AKShare 行业兜底获取 {len(industry_map)} 条，报告期={report_date}")
                    return industry_map
            except Exception as e:
                logger.warning(f"AKShare 行业报告期 {report_date} 获取失败: {e}")
        return {}

    async def _get_akshare_roe_map(self) -> Dict[str, float]:
        cached = self._roe_cache.get("data")
        if cached is not None and time.time() - self._roe_cache.get("ts", 0) < self._roe_cache_ttl:
            return cached

        data = await asyncio.to_thread(self._fetch_akshare_roe_map)
        self._roe_cache = {"ts": time.time(), "data": data}
        return data

    def _fetch_akshare_roe_map(self) -> Dict[str, float]:
        try:
            import akshare as ak
        except ImportError:
            logger.warning("AKShare 未安装，无法获取 ROE 兜底数据")
            return {}

        date_candidates = self._financial_report_date_candidates()
        for report_date in date_candidates:
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = ak.stock_yjbb_em(date=report_date)
                if df is None or getattr(df, "empty", True):
                    continue
                code_col = "股票代码" if "股票代码" in df.columns else None
                roe_col = "净资产收益率" if "净资产收益率" in df.columns else None
                if not code_col or not roe_col:
                    continue
                roe_map: Dict[str, float] = {}
                for _, row in df.iterrows():
                    code = str(row.get(code_col) or "").zfill(6)
                    roe = self._safe_float(row.get(roe_col))
                    if code and roe is not None:
                        roe_map[code] = roe
                if roe_map:
                    logger.info(f"✅ AKShare ROE 兜底获取 {len(roe_map)} 条，报告期={report_date}")
                    return roe_map
            except Exception as e:
                logger.warning(f"AKShare ROE 报告期 {report_date} 获取失败: {e}")
        return {}

    def _financial_report_date_candidates(self) -> List[str]:
        now = datetime.now()
        quarters = ["0331", "0630", "0930", "1231"]
        candidates = []
        for year in [now.year, now.year - 1]:
            for q in reversed(quarters):
                date = f"{year}{q}"
                if date <= now.strftime("%Y%m%d"):
                    candidates.append(date)
        # 财报披露可能滞后，保留最近 6 个报告期依次尝试。
        return candidates[:6]

    def _matches_conditions(self, row: Dict[str, Any], conditions: List[Dict[str, Any]]) -> bool:
        for condition in conditions:
            field = self.basic_fields.get(self._condition_field(condition), self._condition_field(condition))
            operator = self._condition_operator(condition)
            expected = self._condition_value(condition)
            actual = row.get(field)
            if not self._match_condition(actual, operator, expected):
                return False
        return True

    def _match_condition(self, actual: Any, operator: Optional[str], expected: Any) -> bool:
        if actual is None:
            return False
        if isinstance(actual, float) and math.isnan(actual):
            return False

        if operator == "between" and isinstance(expected, list) and len(expected) == 2:
            actual_num = self._safe_float(actual)
            low = self._safe_float(expected[0])
            high = self._safe_float(expected[1])
            return actual_num is not None and low is not None and high is not None and low <= actual_num <= high
        if operator in {">", "<", ">=", "<="}:
            actual_num = self._safe_float(actual)
            expected_num = self._safe_float(expected)
            if actual_num is None or expected_num is None:
                return False
            if operator == ">":
                return actual_num > expected_num
            if operator == "<":
                return actual_num < expected_num
            if operator == ">=":
                return actual_num >= expected_num
            return actual_num <= expected_num
        if operator == "==":
            return str(actual) == str(expected)
        if operator == "!=":
            return str(actual) != str(expected)
        if operator == "in":
            values = expected if isinstance(expected, list) else [expected]
            return str(actual) in {str(v) for v in values}
        if operator == "not_in":
            values = expected if isinstance(expected, list) else [expected]
            return str(actual) not in {str(v) for v in values}
        if operator == "contains":
            return str(expected).lower() in str(actual).lower()
        return False

    def _sort_external_rows(
        self,
        rows: List[Dict[str, Any]],
        order_by: Optional[List[Dict[str, str]]]
    ) -> List[Dict[str, Any]]:
        sort_items = order_by or [{"field": "total_mv", "direction": "desc"}]
        for order in reversed(sort_items):
            field = self.basic_fields.get(order.get("field"), order.get("field"))
            reverse = order.get("direction", "desc").lower() == "desc"
            direction = -1 if reverse else 1
            rows.sort(
                key=lambda item: (
                    item.get(field) is None,
                    direction * (self._safe_float(item.get(field)) if self._safe_float(item.get(field)) is not None else 0),
                )
            )
        return rows

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            if value is None or value == "" or value == "--":
                return None
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return None
            return number
        except (TypeError, ValueError):
            return None

    def _safe_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "--"}:
            return ""
        return text

    async def get_external_industries(self) -> List[Dict[str, Any]]:
        industry_map = await self._get_baostock_industry_map()
        if not industry_map:
            return []
        counts: Dict[str, int] = {}
        for industry in industry_map.values():
            counts[industry] = counts.get(industry, 0) + 1
        return [
            {"value": name, "label": name, "count": count}
            for name, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ]
    
    async def _build_query(self, conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """构建MongoDB查询条件"""
        query = {}

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            value = condition.get("value") if isinstance(condition, dict) else condition.value

            logger.info(f"🔍 [_build_query] 处理条件: field={field}, operator={operator}, value={value}")

            # 映射字段名
            db_field = self.basic_fields.get(field)
            if not db_field:
                logger.warning(f"⚠️ [_build_query] 字段 {field} 不在 basic_fields 映射中，跳过")
                continue

            logger.info(f"✅ [_build_query] 字段映射: {field} -> {db_field}")
            
            # 处理不同操作符
            if operator == "between":
                # between操作需要两个值
                if isinstance(value, list) and len(value) == 2:
                    query[db_field] = {
                        "$gte": value[0],
                        "$lte": value[1]
                    }
            elif operator == "contains":
                # 字符串包含（不区分大小写）
                query[db_field] = {
                    "$regex": str(value),
                    "$options": "i"
                }
            elif operator in self.operators:
                # 标准操作符
                mongo_op = self.operators[operator]
                query[db_field] = {mongo_op: value}
            
        return query
    
    def _build_sort_conditions(self, order_by: Optional[List[Dict[str, str]]]) -> List[Tuple[str, int]]:
        """构建排序条件"""
        if not order_by:
            # 默认按总市值降序排序
            return [("total_mv", -1)]
        
        sort_conditions = []
        for order in order_by:
            field = order.get("field")
            direction = order.get("direction", "desc")
            
            # 映射字段名
            db_field = self.basic_fields.get(field)
            if not db_field:
                continue
            
            # 映射排序方向
            sort_direction = -1 if direction.lower() == "desc" else 1
            sort_conditions.append((db_field, sort_direction))
        
        return sort_conditions
    
    async def _enrich_with_financial_data(self, results: List[Dict[str, Any]], codes: List[str]) -> None:
        """
        批量查询财务数据并填充到结果中

        Args:
            results: 筛选结果列表
            codes: 股票代码列表
        """
        try:
            db = get_mongo_db()
            financial_collection = db['stock_financial_data']

            # 🔥 获取数据源优先级配置
            from app.core.unified_config import UnifiedConfigManager
            config = UnifiedConfigManager()
            data_source_configs = await config.get_data_source_configs_async()

            # 提取启用的数据源，按优先级排序
            enabled_sources = [
                ds.type.lower() for ds in data_source_configs
                if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
            ]

            if not enabled_sources:
                enabled_sources = ['tushare', 'akshare', 'baostock']

            # 优先使用优先级最高的数据源
            preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

            # 批量查询最新的财务数据
            # 按 code 分组，取每个 code 的最新一期数据（只查询优先级最高的数据源）
            pipeline = [
                {"$match": {"code": {"$in": codes}, "data_source": preferred_source}},
                {"$sort": {"code": 1, "report_period": -1}},
                {"$group": {
                    "_id": "$code",
                    "roe": {"$first": "$roe"},
                    "roa": {"$first": "$roa"},
                    "netprofit_margin": {"$first": "$netprofit_margin"},
                    "gross_margin": {"$first": "$gross_margin"},
                }}
            ]

            financial_data_map = {}
            async for doc in financial_collection.aggregate(pipeline):
                code = doc.get("_id")
                financial_data_map[code] = {
                    "roe": doc.get("roe"),
                    "roa": doc.get("roa"),
                    "netprofit_margin": doc.get("netprofit_margin"),
                    "gross_margin": doc.get("gross_margin"),
                }

            # 填充财务数据到结果中
            for result in results:
                code = result.get("code")
                if code in financial_data_map:
                    financial_data = financial_data_map[code]
                    # 只更新 ROE（如果 stock_basic_info 中没有的话）
                    if result.get("roe") is None:
                        result["roe"] = financial_data.get("roe")
                    # 可以添加更多财务指标
                    # result["roa"] = financial_data.get("roa")
                    # result["netprofit_margin"] = financial_data.get("netprofit_margin")

            logger.debug(f"✅ 已填充 {len(financial_data_map)} 条财务数据")

        except Exception as e:
            logger.warning(f"⚠️ 填充财务数据失败: {e}")
            # 不抛出异常，允许继续返回基础数据

    def _format_result(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """格式化查询结果，统一使用后端字段名"""
        # 根据股票代码推断市场类型
        code = doc.get("code", "")
        market_type = "A股"  # 默认A股
        if code:
            if code.startswith("6"):
                market_type = "A股"  # 上海
            elif code.startswith(("0", "3")):
                market_type = "A股"  # 深圳
            elif code.startswith("8") or code.startswith("4"):
                market_type = "A股"  # 北交所

        result = {
            # 基础信息
            "code": doc.get("code"),
            "name": doc.get("name"),
            "industry": doc.get("industry"),
            "area": doc.get("area"),
            "market": market_type,  # 市场类型（A股、美股、港股）
            "board": doc.get("market"),  # 板块（主板、创业板、科创板等）
            "exchange": doc.get("sse"),  # 交易所（上海证券交易所、深圳证券交易所等）
            "list_date": doc.get("list_date"),

            # 市值信息（亿元）
            "total_mv": doc.get("total_mv"),
            "circ_mv": doc.get("circ_mv"),

            # 财务指标
            "pe": doc.get("pe"),
            "pb": doc.get("pb"),
            "pe_ttm": doc.get("pe_ttm"),
            "pb_mrq": doc.get("pb_mrq"),
            "roe": doc.get("roe"),

            # 交易指标
            "turnover_rate": doc.get("turnover_rate"),
            "volume_ratio": doc.get("volume_ratio"),

            # 交易数据（从视图中获取，视图已包含实时行情数据）
            "close": doc.get("close"),              # 收盘价
            "pct_chg": doc.get("pct_chg"),          # 涨跌幅(%)
            "amount": doc.get("amount"),            # 成交额
            "volume": doc.get("volume"),            # 成交量
            "open": doc.get("open"),                # 开盘价
            "high": doc.get("high"),                # 最高价
            "low": doc.get("low"),                  # 最低价

            # 技术指标（基础信息筛选时为None）
            "ma20": None,
            "rsi14": None,
            "kdj_k": None,
            "kdj_d": None,
            "kdj_j": None,
            "dif": None,
            "dea": None,
            "macd_hist": None,

            # 元数据
            "source": doc.get("source", "database"),
            "updated_at": doc.get("updated_at"),
        }
        
        # 移除None值
        return {k: v for k, v in result.items() if v is not None}
    
    async def get_field_statistics(self, field: str) -> Dict[str, Any]:
        """
        获取字段的统计信息
        
        Args:
            field: 字段名
            
        Returns:
            Dict: 统计信息 {min, max, avg, count}
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return {}
            
            db = get_mongo_db()
            collection = db[self.collection_name]
            
            # 使用聚合管道获取统计信息
            pipeline = [
                {"$match": {db_field: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": None,
                    "min": {"$min": f"${db_field}"},
                    "max": {"$max": f"${db_field}"},
                    "avg": {"$avg": f"${db_field}"},
                    "count": {"$sum": 1}
                }}
            ]
            
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                stats = result[0]
                avg_value = stats.get("avg")
                return {
                    "field": field,
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "avg": round(avg_value, 2) if avg_value is not None else None,
                    "count": stats.get("count", 0)
                }
            
            return {"field": field, "count": 0}
            
        except Exception as e:
            logger.error(f"获取字段统计失败: {e}")
            return {"field": field, "error": str(e)}
    
    def _separate_conditions(self, conditions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        分离基础信息条件和实时行情条件

        Args:
            conditions: 所有筛选条件

        Returns:
            Tuple[基础信息条件列表, 实时行情条件列表]
        """
        # 实时行情字段（需要从 market_quotes 查询）
        quote_fields = {"pct_chg", "amount", "close", "volume"}

        basic_conditions = []
        quote_conditions = []

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            if field in quote_fields:
                quote_conditions.append(condition)
            else:
                basic_conditions.append(condition)

        return basic_conditions, quote_conditions

    async def _filter_by_quotes(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        quote_conditions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        根据实时行情数据进行二次筛选

        Args:
            results: 初步筛选结果
            codes: 股票代码列表
            quote_conditions: 实时行情筛选条件

        Returns:
            List[Dict]: 筛选后的结果
        """
        try:
            db = get_mongo_db()
            quotes_collection = db['market_quotes']

            # 批量查询实时行情数据
            quotes_cursor = quotes_collection.find({"code": {"$in": codes}})
            quotes_map = {}
            async for quote in quotes_cursor:
                code = quote.get("code")
                quotes_map[code] = {
                    "close": quote.get("close"),
                    "pct_chg": quote.get("pct_chg"),
                    "amount": quote.get("amount"),
                    "volume": quote.get("volume"),
                }

            logger.info(f"📊 查询到 {len(quotes_map)} 只股票的实时行情数据")

            # 过滤结果
            filtered_results = []
            for result in results:
                code = result.get("code")
                quote_data = quotes_map.get(code)

                if not quote_data:
                    # 没有实时行情数据，跳过
                    continue

                # 检查是否满足所有实时行情条件
                match = True
                for condition in quote_conditions:
                    field = condition.get("field") if isinstance(condition, dict) else condition.field
                    operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
                    value = condition.get("value") if isinstance(condition, dict) else condition.value

                    field_value = quote_data.get(field)
                    if field_value is None:
                        match = False
                        break

                    # 检查条件
                    if operator == "between" and isinstance(value, list) and len(value) == 2:
                        if not (value[0] <= field_value <= value[1]):
                            match = False
                            break
                    elif operator == ">":
                        if not (field_value > value):
                            match = False
                            break
                    elif operator == "<":
                        if not (field_value < value):
                            match = False
                            break
                    elif operator == ">=":
                        if not (field_value >= value):
                            match = False
                            break
                    elif operator == "<=":
                        if not (field_value <= value):
                            match = False
                            break

                if match:
                    # 将实时行情数据合并到结果中
                    result.update(quote_data)
                    filtered_results.append(result)

            logger.info(f"✅ 实时行情筛选完成: 筛选前={len(results)}, 筛选后={len(filtered_results)}")
            return filtered_results

        except Exception as e:
            logger.error(f"❌ 实时行情筛选失败: {e}")
            # 如果失败，返回原始结果
            return results

    async def get_available_values(self, field: str, limit: int = 100) -> List[str]:
        """
        获取字段的可选值列表（用于枚举类型字段）
        
        Args:
            field: 字段名
            limit: 返回数量限制
            
        Returns:
            List[str]: 可选值列表
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return []
            
            db = get_mongo_db()
            collection = db[self.collection_name]
            
            # 获取字段的不重复值
            values = await collection.distinct(db_field)
            
            # 过滤None值并排序
            values = [v for v in values if v is not None]
            values.sort()
            
            return values[:limit]
            
        except Exception as e:
            logger.error(f"获取字段可选值失败: {e}")
            return []


# 全局服务实例
_database_screening_service: Optional[DatabaseScreeningService] = None


def get_database_screening_service() -> DatabaseScreeningService:
    """获取数据库筛选服务实例"""
    global _database_screening_service
    if _database_screening_service is None:
        _database_screening_service = DatabaseScreeningService()
    return _database_screening_service
