import asyncio
import logging
from typing import Callable, List, Optional, Union

import websockets

logger = logging.getLogger("collabvm")


# --- Guacamole Encoding / Decoding Utilities ---

def encode_instruction(elements: List[str]) -> str:
    """
    Encodes a list of string elements into a Guacamole instruction format:
    'length.element,length.element;'
    """
    encoded_parts = [f"{len(elem)}.{elem}" for elem in elements]
    return ",".join(encoded_parts) + ";"


def decode_instruction(data: str) -> List[List[str]]:
    """
    Parses incoming raw Guacamole protocol strings into a list of parsed instruction tokens.
    Handles multiple instructions concatenated together in a single WebSocket frame.
    """
    instructions = []
    i = 0
    length = len(data)

    while i < length:
        current_instruction = []
        while i < length:
            # Read length prefix until '.'
            dot_idx = data.find('.', i)
            if dot_idx == -1:
                break
            
            elem_len = int(data[i:dot_idx])
            start_pos = dot_idx + 1
            end_pos = start_pos + elem_len
            
            element = data[start_pos:end_pos]
            current_instruction.append(element)
            
            delimiter = data[end_pos]
            i = end_pos + 1
            
            if delimiter == ';':
                instructions.append(current_instruction)
                break
            elif delimiter == ',':
                continue
            else:
                raise ValueError(f"Unexpected Guacamole delimiter '{delimiter}' at index {end_pos}")
                
    return instructions


# --- CollabVM Client Library ---

class CollabVMClient:
    def __init__(self, url: str):
        self.url = url
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        
        # State tracking
        self.auth_url: Optional[str] = None
        self.username: Optional[str] = None
        self.vms: List[dict] = []
        self.connected_vm: Optional[str] = None
        self.rank: int = 0  # 0: Guest, 1: User, 2: Moderator, 3: Admin
        
        # Event callbacks
        self.on_chat: Optional[Callable[[str, str], None]] = None  # (username, message)
        self.on_user_join: Optional[Callable[[str], None]] = None
        self.on_user_leave: Optional[Callable[[str], None]] = None
        self.on_rename: Optional[Callable[[str, str], None]] = None  # (old_name, new_name)
        self.on_raw_instruction: Optional[Callable[[List[str]], None]] = None

        self._receive_task: Optional[asyncio.Task] = None
        self._connected_event = asyncio.Event()

    async def connect(self, requested_username: Optional[str] = None):
        """Establishes WebSocket connection with subprotocol 'guacamole' and starts receiver task."""
        logger.info(f"Connecting to {self.url}...")
        self.ws = await websockets.connect(self.url, subprotocols=['guacamole'])
        
        # Start listening for incoming frames
        self._receive_task = asyncio.create_task(self._receive_loop())

        # Request initial list of VMs
        await self.send(["list"])
        
        # Request a username/guest name
        await self.rename(requested_username)

    async def disconnect(self):
        """Closes the WebSocket connection gracefully."""
        if self._receive_task:
            self._receive_task.cancel()
        if self.ws:
            await self.ws.close()
            logger.info("Disconnected from server.")

    async def send(self, instruction: List[str]):
        """Encodes and sends an instruction token list over the WebSocket connection."""
        if self.ws:
            raw_msg = encode_instruction(instruction)
            await self.ws.send(raw_msg)

    # --- Protocol Actions ---

    async def rename(self, new_username: Optional[str] = None):
        """
        Request a username change (or initial assignment).
        Sending no arguments requests the server to assign a random guest name.
        """
        msg = ["rename"]
        if new_username:
            msg.append(new_username)
        await self.send(msg)

    async def login_account(self, token: str):
        """
        Logs into an account using an auth token obtained from the central CollabVM auth server.
        Instruction: ['login', token]
        """
        await self.send(["login", token])

    async def login_staff(self, password: str):
        """
        Logs in as a staff member (mod/admin) using a legacy master password.
        Instruction: ['admin', '2', password]
        """
        await self.send(["admin", "2", password])

    async def connect_vm(self, vm_id: str):
        """Connects to a specific Virtual Machine by its node ID."""
        await self.send(["connect", vm_id])
        self.connected_vm = vm_id

    async def send_chat(self, message: str):
        """Sends a chat message to the connected server/VM."""
        await self.send(["chat", message])

    async def take_turn(self):
        """Requests a turn to control the VM."""
        await self.send(["turn", "1"])

    async def release_turn(self):
        """Releases control of the VM."""
        await self.send(["turn", "0"])

    async def send_mouse(self, x: int, y: int, mask: int = 0):
        """Sends a mouse movement or click event."""
        await self.send(["mouse", str(x), str(y), str(mask)])

    async def send_key(self, keysym: int, pressed: bool):
        """Sends a keyboard event (keysym as integer, pressed boolean)."""
        state = "1" if pressed else "0"
        await self.send(["key", str(keysym), state])

    # --- Incoming Event Processing ---

    async def _receive_loop(self):
        """Main receiving loop that handles incoming WebSocket messages."""
        try:
            async for message in self.ws:
                instructions = decode_instruction(message)
                for inst in instructions:
                    await self._handle_instruction(inst)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in receive loop: {e}")

    async def _handle_instruction(self, inst: List[str]):
        if not inst:
            return

        opcode = inst[0]

        # Trigger raw instruction callback
        if self.on_raw_instruction:
            self.on_raw_instruction(inst)

        # 1. Heartbeat / Keepalive
        if opcode == "nop":
            # Must respond with standard nop instruction immediately
            await self.send(["nop"])

        # 2. Auth Announcement
        elif opcode == "auth":
            if len(inst) > 1:
                self.auth_url = inst[1]
                logger.info(f"Server announced account auth server: {self.auth_url}")

        # 3. List of Available VMs
        elif opcode == "list":
            self.vms.clear()
            # Parameters repeat in groups of 3: [id, name, base64_thumbnail]
            for i in range(1, len(inst), 3):
                if i + 2 < len(inst):
                    self.vms.append({
                        "id": inst[i],
                        "name": inst[i + 1],
                        "thumbnail": inst[i + 2]
                    })
            logger.info(f"Loaded {len(self.vms)} available VM(s).")

        # 4. Client Rename / Initial Username ACK
        elif opcode == "rename":
            # Response format: ['rename', status, old_username, new_username, rank]
            if len(inst) >= 5:
                status, old_name, new_name, rank = inst[1], inst[2], inst[3], inst[4]
                if status == "0":
                    self.username = new_name
                    self.rank = int(rank)
                    logger.info(f"Username changed to: {self.username} (Rank: {self.rank})")
                    if self.on_rename:
                        self.on_rename(old_name, new_name)

        # 5. User Login Response (Account auth)
        elif opcode == "login":
            # Response format: ['login', status, username, rank]
            if len(inst) >= 4:
                status, username, rank = inst[1], inst[2], inst[3]
                if status == "0":
                    self.username = username
                    self.rank = int(rank)
                    logger.info(f"Logged in successfully as {self.username} (Rank: {self.rank})")
                else:
                    logger.warning(f"Account login failed with code {status}")

        # 6. Staff Login Response
        elif opcode == "admin":
            # Response format: ['admin', sub_opcode, status, ...]
            if len(inst) >= 3 and inst[1] == "2":
                status = inst[2]
                if status == "1":
                    logger.info("Staff login successful!")
                else:
                    logger.warning("Staff login failed!")

        # 7. Chat Message Received
        elif opcode == "chat":
            if len(inst) >= 3:
                user, msg = inst[1], inst[2]
                logger.info(f"[{user}]: {msg}")
                if self.on_chat:
                    self.on_chat(user, msg)

        # 8. User Management Events
        elif opcode == "adduser":
            if len(inst) >= 2 and self.on_user_join:
                self.on_user_join(inst[1])
                
        elif opcode == "remuser":
            if len(inst) >= 2 and self.on_user_leave:
                self.on_user_leave(inst[1])