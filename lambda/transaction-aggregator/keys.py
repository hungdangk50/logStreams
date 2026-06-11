"""DynamoDB key builders for LogStreamTransactions single-table design."""


def txn_pk(transaction_id: str) -> str:
    return f"TXN#{transaction_id}"


def meta_sk() -> str:
    return "META"


def step_sk(step_order: int, step_name: str) -> str:
    return f"STEP#{step_order:02d}#{step_name}"


def attempt_sk(step_order: int, step_name: str, attempt_number: int) -> str:
    return f"ATTEMPT#{step_order:02d}#{step_name}#{attempt_number:03d}"


def log_sk(trace_id: str, chunk_seq: int) -> str:
    return f"LOG#{trace_id}#{chunk_seq:04d}"


def gsi2_pk(trace_id: str) -> str:
    return f"TRACE#{trace_id}"


def gsi2_sk(transaction_id: str) -> str:
    return f"TXN#{transaction_id}"


def gsi1_pk_running() -> str:
    return "STATUS#RUNNING"
