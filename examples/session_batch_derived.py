from __future__ import annotations

import asyncio

from coalestra import (
    CallableBatchSource,
    CallableDerivedSource,
    ResourceKey,
    SnapshotBuilder,
)

ALL_USERS = ResourceKey("users", "all")


def user(user_id: str) -> ResourceKey:
    return ResourceKey("users", "one", user_id)


def profile(user_id: str) -> ResourceKey:
    return ResourceKey("profiles", "summary", user_id)


async def fetch_documents(keys, _context):
    # One remote operation can return several requested resources.
    return {key: {"id": key.subject, "name": f"User {key.subject}"} for key in keys}


def derive_profile(key, dependencies, _context):
    document = dependencies.value(user(key.subject), dict)
    return {"display_name": document["name"]}


async def main() -> None:
    builder = SnapshotBuilder(
        [
            CallableDerivedSource(
                name="profile-view",
                priority=100,
                supports=lambda key: key.namespace == "profiles",
                dependencies=lambda key: (user(key.subject),),
                deriver=derive_profile,
            ),
            CallableBatchSource(
                name="user-api",
                priority=10,
                supports=lambda key: key.namespace == "users" and key.name == "one",
                fetcher=fetch_documents,
            ),
        ]
    )

    async with builder.session(snapshot_id="request-42") as session:
        first = await session.resolve([user("1"), user("2")])
        print(first.snapshot_id, first.value(user("1")))

        final = await session.resolve([profile("1"), profile("2")])
        print(final.value(profile("1")))
        print(final.value(profile("2")))


if __name__ == "__main__":
    asyncio.run(main())
