class IdempotencyConflictError(Exception):
    pass


class WebhookError(Exception):
    pass


class WebhookRejectedError(WebhookError):
    pass


class GatewayRejectedError(Exception):
    pass


class ChargePendingError(Exception):
    pass


class PaymentNotFoundError(Exception):
    pass
