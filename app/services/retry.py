from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Попытки и паузы одного этапа обработки: у списания и у вебхука по max_attempts."""

    max_attempts: int
    first_delay: float
    factor: float = 5

    def delay(self, attempt: int) -> float:
        return self.first_delay * self.factor ** (attempt - 1)

    @property
    def delays(self) -> list[float]:
        return [self.delay(n) for n in range(1, max(self.max_attempts, 2))]

    def round_up(self, wait: float) -> float:
        return next((d for d in self.delays if d >= wait), self.delays[-1])

    def exhausted(self, used: int) -> bool:
        return used >= self.max_attempts

    def interrupted(self, used: int) -> bool:
        return used > self.max_attempts
