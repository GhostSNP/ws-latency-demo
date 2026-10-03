"""Клиент: шлёт чанки раз в 20 мс (как поток аудио 16 кГц, 16 бит, mono:
20 мс = 320 отсчётов = 640 байт) и меряет время туда-обратно (RTT).
"""
import asyncio
import statistics
import struct
import sys
import time
from websockets.asyncio.client import connect

URI = "ws://127.0.0.1:8765"
CHUNK_BYTES = 640        # 20 мс аудио
INTERVAL = 0.020         # 20 мс между чанками
N_CHUNKS = 500           # ~10 секунд
HEADER = struct.Struct("!Id")  # номер чанка + время отправки


async def sender(ws):
    padding = bytes(CHUNK_BYTES - HEADER.size)
    next_t = time.perf_counter()
    for seq in range(N_CHUNKS):
        await ws.send(HEADER.pack(seq, time.perf_counter()) + padding)
        next_t += INTERVAL                       # держим ровный ритм
        await asyncio.sleep(max(0, next_t - time.perf_counter()))


async def receiver(ws, rtts):
    while len(rtts) < N_CHUNKS:
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=2)
        except asyncio.TimeoutError:
            break                                # остальные считаем потерянными
        _, sent_at = HEADER.unpack_from(msg)
        rtts.append((time.perf_counter() - sent_at) * 1000)


def report(rtts):
    if not rtts:
        print("Нет ответов")
        return
    s = sorted(rtts)
    pct = lambda p: s[min(len(s) - 1, int(len(s) * p))]
    print(f"Получено: {len(rtts)}/{N_CHUNKS} (потеряно {N_CHUNKS - len(rtts)})")
    print(f"RTT, мс: mean={statistics.mean(s):.2f}  p50={pct(.50):.2f}  "
          f"p95={pct(.95):.2f}  p99={pct(.99):.2f}  max={s[-1]:.2f}")


async def main(uri=URI):
    rtts = []
    async with connect(uri) as ws:
        await asyncio.gather(sender(ws), receiver(ws, rtts))
    report(rtts)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else URI))
