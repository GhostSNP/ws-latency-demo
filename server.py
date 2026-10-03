"""Echo-сервер на asyncio + websockets.
Принимает бинарные «аудио-чанки» и сразу отправляет их обратно.
"""
import asyncio
from websockets.asyncio.server import serve


async def handler(ws):
    async for chunk in ws:      # каждый чанк -> сразу обратно
        await ws.send(chunk)


async def main(host="127.0.0.1", port=8765):
    async with serve(handler, host, port):
        print(f"Сервер слушает ws://{host}:{port}")
        await asyncio.Future()  # работать вечно


if __name__ == "__main__":
    asyncio.run(main())
