import argparse
import asyncio
from uuid import UUID

from app.infrastructure.config import Settings
from app.infrastructure.db.engine import create_db_engine, session_factory
from app.infrastructure.db.repository import unit_of_work
from app.services.dead_letters import revive


async def main() -> None:
    parser = argparse.ArgumentParser(description="Return payments from the DLQ to processing")
    parser.add_argument("payment_ids", nargs="*", type=UUID)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if not args.payment_ids and not args.all:
        parser.error("pass payment ids or --all")

    settings = Settings()
    engine = create_db_engine(settings, pool_size=1)
    try:
        uow = unit_of_work(session_factory(engine), settings.db_timeout)
        revived = await revive(uow, None if args.all else args.payment_ids)
    finally:
        await engine.dispose()
    print(f"Requeued {len(revived)} payments")
    for payment_id in revived:
        print(payment_id)


if __name__ == "__main__":
    asyncio.run(main())
