"""
Demonstration of Futures in Python:
1. concurrent.futures.Future (Thread-based concurrency)
2. asyncio.Future (Event-loop based asynchronous concurrency)
3. Bridging both worlds (loop.run_in_executor)
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import time


# =====================================================================
# 1. concurrent.futures.Future (Thread-based)
# =====================================================================
def slow_thread_task(x: int) -> int:
    tid = threading.get_ident()
    print(f"  [Worker Thread (id={tid})] Computing {x} * 10...")
    time.sleep(1)
    return x * 10


def demo_concurrent_futures():
    print("=" * 65)
    print("DEMO 1: concurrent.futures.Future (Thread-based)")
    print("=" * 65)

    main_tid = threading.get_ident()
    print(f"Main Thread ID: {main_tid}")

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(slow_thread_task, 5)

        # A concurrent.futures.Future is purely OS-thread based.
        # It has NO concept of an event loop!
        has_loop = hasattr(future, "get_loop")
        print(f"Future object: {future}")
        print(f"Does concurrent.futures.Future have an event loop? {has_loop}")
        if not has_loop:
            print("👉 Event Loop ID: None (Threads use threading.Condition, not an event loop)")

        result = future.result()
        print(f"Got result: {result}\n")


# =====================================================================
# 2. asyncio.Future (Event-loop based)
# =====================================================================
async def async_worker(fut: asyncio.Future):
    tid = threading.get_ident()
    print(f"  [Async Task (thread={tid})] Resolving future on the event loop...")
    await asyncio.sleep(1)
    fut.set_result({"status": "success", "message": "Payload from server"})


async def demo_asyncio_future():
    print("=" * 65)
    print("DEMO 2: asyncio.Future (Event-loop based)")
    print("=" * 65)

    current_loop = asyncio.get_running_loop()
    main_tid = threading.get_ident()

    fut: asyncio.Future = current_loop.create_future()

    # An asyncio.Future is strictly bound to its creating event loop!
    future_loop = fut.get_loop()
    print(f"Thread ID:            {main_tid}")
    print(f"Active Loop ID:       {hex(id(current_loop))} (from get_running_loop)")
    print(f"Future's Loop ID:     {hex(id(future_loop))} (from fut.get_loop())")
    print(f"Is it the same loop?  {future_loop is current_loop}")

    asyncio.create_task(async_worker(fut))
    result = await fut
    print(f"Got result: {result}\n")


# =====================================================================
# 3. Bridging Both: Running Blocking Work in AsyncIO via Executor
# =====================================================================
def blocking_dns_call(domain: str) -> str:
    tid = threading.get_ident()
    print(f"  [Thread Pool (id={tid})] Resolving '{domain}'...")
    time.sleep(1)
    return f"192.0.2.1 ({domain})"


async def demo_bridge_executor():
    print("=" * 65)
    print("DEMO 3: Bridging Threads & AsyncIO (run_in_executor)")
    print("=" * 65)

    loop = asyncio.get_running_loop()
    print(f"Event Loop ID: {hex(id(loop))}")

    # run_in_executor offloads to a thread pool and wraps the thread future
    # into an asyncio.Future attached to THIS event loop!
    result = await loop.run_in_executor(None, blocking_dns_call, "google.com")
    print(f"Event Loop received result: {result}\n")


# =====================================================================
# Main Runner
# =====================================================================
async def main():
    demo_concurrent_futures()
    await demo_asyncio_future()
    await demo_bridge_executor()


if __name__ == "__main__":
    asyncio.run(main())
