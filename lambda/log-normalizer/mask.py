"""
LogStream — PII Masking
========================

Che giấu thông tin nhạy cảm (Personally Identifiable Information) trước khi
log được lưu trữ hoặc truyền downstream.

Chiến lược mask
---------------
1. Field-based masking
   Các field có tên nhạy cảm (email, password, token, ...) → thay toàn bộ
   giá trị bằng ***MASKED*** bất kể nội dung.

2. Pattern-based masking (trên string)
   Quét regex trên nội dung text:
       - Email
       - Số điện thoại
       - Số thẻ tín dụng (13–19 chữ số)
       - SSN (Mỹ: xxx-xx-xxxx)

3. Recursive
   Áp dụng đệ quy trên dict và list lồng nhau.

Giá trị mask
------------
    MASK = "***MASKED***"

Lưu ý
-----
    - Mask chạy TRƯỚC khi extract field → metadata downstream cũng đã được mask.
    - Regex phone/card có thể false-positive trên số dài trong message thường.
    - Bổ sung PII_FIELD_NAMES hoặc pattern khi có thêm loại dữ liệu nhạy cảm.
"""

import re
from typing import Any

# Pattern nhận diện PII trong free-text
EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)
PHONE_PATTERN = re.compile(
    r"\b(?:\+?\d{1,3}[-.\s]?)?(?:\(\d{2,4}\)|\d{2,4})[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b"
)
CREDIT_CARD_PATTERN = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

# Tên field JSON — mask toàn bộ value (case-insensitive match)
PII_FIELD_NAMES = frozenset({
    "email",
    "phone",
    "phoneNumber",
    "mobile",
    "password",
    "passwd",
    "secret",
    "token",
    "accessToken",
    "refreshToken",
    "creditCard",
    "cardNumber",
    "cvv",
    "ssn",
    "nationalId",
})

MASK = "***MASKED***"


def mask_string(value: str) -> str:
    """
    Áp dụng tất cả regex pattern lên một chuỗi.

    Args:
        value: Chuỗi gốc (message, field text, ...).

    Returns:
        Chuỗi đã thay thế các pattern PII bằng MASK.
    """
    masked = EMAIL_PATTERN.sub(MASK, value)
    masked = PHONE_PATTERN.sub(MASK, masked)
    masked = CREDIT_CARD_PATTERN.sub(MASK, masked)
    masked = SSN_PATTERN.sub(MASK, masked)
    return masked


def mask_value(key: str | None, value: Any) -> Any:
    """
    Mask một giá trị đơn lẻ theo type và tên field.

    Args:
        key: Tên field JSON (None nếu phần tử trong list).
        value: Giá trị cần mask.

    Returns:
        Giá trị đã mask (string/dict/list giữ nguyên structure).
    """
    if isinstance(value, str):
        if key and key.lower() in PII_FIELD_NAMES:
            return MASK
        return mask_string(value)

    if isinstance(value, dict):
        return mask_dict(value)

    if isinstance(value, list):
        return [mask_value(None, item) for item in value]

    return value


def mask_dict(data: dict[str, Any]) -> dict[str, Any]:
    """
    Mask đệ quy toàn bộ dict log payload.

    Args:
        data: Dict parsed từ log message JSON.

    Returns:
        Dict mới với mọi PII đã được che.
    """
    return {key: mask_value(key, value) for key, value in data.items()}
