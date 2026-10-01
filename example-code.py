import asyncio
import logging
from collabvm import CollabVMClient

logging.basicConfig(level=logging.INFO)

async def main():
    # Instantiate client targeting a CollabVM WebSocket endpoint
    client = CollabVMClient("wss://computernewb.com/collab-vm/vm0")

    # Define event handlers
    def handle_chat(username: str, message: str):
        print(f"Chat Message -> {username}: {message}")

    client.on_chat = handle_chat

    # Connect to server and request guest/custom name
    await client.connect(requested_username="PythonBot")

    # Wait a bit for initial handshake instructions (auth announcement, list, rename)
    await asyncio.sleep(2)

    # Option A: Login via Centralized CollabVM Auth (if auth_url is announced)
    # token = "your_auth_token_from_auth_url"
    # await client.login_account(token)

    # Option B: Login as Staff (if staff auth is enabled)
    # await client.login_staff("admin_password")

    # Option C: Change username via Guest/Change system
    await client.rename("NewCoolName")

    # Connect to the first available VM node
    if client.vms:
        vm_id = client.vms[0]["id"]
        print(f"Connecting to VM: {vm_id}")
        await client.connect_vm(vm_id)

    # Send a chat message
    await asyncio.sleep(1)
    await client.send_chat("Hello from Python CollabVM client!")

    # Keep alive for 30 seconds
    await asyncio.sleep(30)
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())