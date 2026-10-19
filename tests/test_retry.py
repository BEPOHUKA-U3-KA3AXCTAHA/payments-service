import pytest

from app.services.retry import RetryPolicy


def test_delays():
    policy = RetryPolicy(max_attempts=3, first_delay=5)
    assert [policy.delay(n) for n in (1, 2, 3)] == [5, 25, 125]
    assert policy.delays == [5, 25]
    assert RetryPolicy(max_attempts=1, first_delay=5).delays == [5]


@pytest.mark.parametrize(
    ("used", "exhausted", "interrupted"), [(1, False, False), (3, True, False), (4, True, True)]
)
def test_attempts_left(used: int, exhausted: bool, interrupted: bool):
    policy = RetryPolicy(max_attempts=3, first_delay=5)
    assert policy.exhausted(used) is exhausted
    assert policy.interrupted(used) is interrupted
