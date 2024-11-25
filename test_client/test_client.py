import asyncio
import random


class AsyncClient:
    def __init__(self, client_id: int):
        self.client_id = client_id

    async def __aenter__(self):
        self.reader, self.writer = await asyncio.open_connection('localhost', 6379)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.writer.close()
        await self.writer.wait_closed()

    async def send_message(self, message: str) -> str:
        # Send message
        self.writer.write(message.encode('utf-8'))
        await self.writer.drain()

        # Get response
        response = await self.reader.read(1024)
        return response.decode('utf-8')


async def client_session(client_id: int, message_count: int = 5):
    async with AsyncClient(client_id) as client:
        for i in range(message_count):
            message = f"Message {i} from client {client_id}"
            response = await client.send_message(message)
            print(f"Client {client_id} received: {response}")

            # Random delay to simulate real-world usage
            await asyncio.sleep(random.uniform(0.1, 1.0))


async def main():
    # Start multiple clients
    client_tasks = [
        client_session(i, message_count=random.randint(3, 7))
        for i in range(5)
    ]
    await asyncio.gather(*client_tasks)


if __name__ == "__main__":
    asyncio.run(main())