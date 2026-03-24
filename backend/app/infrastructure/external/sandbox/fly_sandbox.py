from typing import Dict, Any, Optional, List, BinaryIO
import uuid
import httpx
import logging
import asyncio
import io
import time
from async_lru import alru_cache
from app.core.config import get_settings
from app.domain.models.tool_result import ToolResult
from app.domain.external.sandbox import Sandbox
from app.infrastructure.external.browser.playwright_browser import PlaywrightBrowser
from app.infrastructure.external.browser.browser_use_browser import BrowserUseBrowser
from app.domain.external.browser import Browser

logger = logging.getLogger(__name__)

class FlySandbox(Sandbox):
    def __init__(self, machine_id: str, ip: str):
        """Initialize Fly.io sandbox and API interaction client"""
        self.client = httpx.AsyncClient(timeout=600)
        self.machine_id = machine_id
        self.ip = ip
        # Use IPv6 format for Fly internal network
        self.base_url = f"http://[{self.ip}]:8080"
        self._vnc_url = f"ws://[{self.ip}]:5901"
        self._cdp_url = f"http://[{self.ip}]:9222"
    
    @property
    def id(self) -> str:
        """Sandbox ID"""
        return self.machine_id
    
    @property
    def cdp_url(self) -> str:
        return self._cdp_url

    @property
    def vnc_url(self) -> str:
        return self._vnc_url

    @staticmethod
    async def _get_fly_client() -> httpx.AsyncClient:
        settings = get_settings()
        if not settings.fly_api_token:
            raise ValueError("FLY_API_TOKEN is not set")
        
        return httpx.AsyncClient(
            base_url="https://api.machines.dev/v1",
            headers={
                "Authorization": f"Bearer {settings.fly_api_token}",
                "Content-Type": "application/json"
            },
            timeout=60.0
        )

    @staticmethod
    async def _create_task() -> 'FlySandbox':
        """Create a new Fly.io Machine sandbox"""
        settings = get_settings()
        
        if not settings.fly_sandbox_app:
            raise ValueError("FLY_SANDBOX_APP is not set")

        image = settings.sandbox_image or "simpleyyt/manus-sandbox:latest"
        name = f"sandbox-{uuid.uuid4().hex[:8]}"

        try:
            async with await FlySandbox._get_fly_client() as fly_client:
                # Create the machine
                payload = {
                    "name": name,
                    "config": {
                        "image": image,
                        "guest": {
                            "cpu_kind": "shared",
                            "cpus": 2,
                            "memory_mb": 4096
                        },
                        "env": {
                            "SERVICE_TIMEOUT_MINUTES": str(settings.sandbox_ttl_minutes or 30),
                            "CHROME_ARGS": settings.sandbox_chrome_args or "",
                            "HTTPS_PROXY": settings.sandbox_https_proxy or "",
                            "HTTP_PROXY": settings.sandbox_http_proxy or "",
                            "NO_PROXY": settings.sandbox_no_proxy or ""
                        },
                        "auto_destroy": True
                    }
                }

                logger.info(f"Creating Fly Machine for sandbox: {name}")
                response = await fly_client.post(
                    f"/apps/{settings.fly_sandbox_app}/machines",
                    json=payload
                )
                response.raise_for_status()
                machine_data = response.json()
                machine_id = machine_data["id"]
                
                # Wait for the machine to start and get its private IP
                # This might take a few seconds
                private_ip = None
                for _ in range(10):
                    resp = await fly_client.get(f"/apps/{settings.fly_sandbox_app}/machines/{machine_id}")
                    if resp.status_code == 200:
                        m_info = resp.json()
                        if m_info.get("state") == "started":
                            private_ip = m_info.get("private_ip")
                            if private_ip:
                                break
                    await asyncio.sleep(2)

                if not private_ip:
                    # Cleanup if we failed to get an IP
                    await fly_client.delete(f"/apps/{settings.fly_sandbox_app}/machines/{machine_id}?force=true")
                    raise Exception("Machine started but could not get private_ip")

                # Wait a moment for services inside the machine to initialize
                await asyncio.sleep(5)
                
                return FlySandbox(machine_id=machine_id, ip=private_ip)
                
        except Exception as e:
            logger.error(f"Failed to create Fly sandbox: {str(e)}")
            raise Exception(f"Failed to create Fly sandbox: {str(e)}")

    async def ensure_sandbox(self) -> None:
        """Ensure sandbox is ready by checking that all services are RUNNING"""
        max_retries = 30
        retry_interval = 2
        
        for attempt in range(max_retries):
            try:
                response = await self.client.get(f"{self.base_url}/api/v1/supervisor/status")
                response.raise_for_status()
                
                tool_result = ToolResult(**response.json())
                
                if not tool_result.success:
                    logger.warning(f"Supervisor status check failed: {tool_result.message}")
                    await asyncio.sleep(retry_interval)
                    continue
                
                services = tool_result.data or []
                if not services:
                    await asyncio.sleep(retry_interval)
                    continue
                
                all_running = True
                for service in services:
                    if service.get("statename", "") != "RUNNING":
                        all_running = False
                        break
                
                if all_running:
                    logger.info("Fly Sandbox is ready")
                    return
                else:
                    await asyncio.sleep(retry_interval)
                    
            except Exception as e:
                logger.warning(f"Failed to check supervisor status: {str(e)}")
                await asyncio.sleep(retry_interval)
        
        logger.error("Sandbox services failed to start")

    # The rest of the methods are identical to DockerSandbox since they just interact with the HTTP API
    async def exec_command(self, session_id: str, exec_dir: str, command: str) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/shell/exec",
            json={"id": session_id, "exec_dir": exec_dir, "command": command}
        )
        return ToolResult(**response.json())

    async def view_shell(self, session_id: str, console: bool = False) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/shell/view",
            json={"id": session_id, "console": console}
        )
        return ToolResult(**response.json())

    async def wait_for_process(self, session_id: str, seconds: Optional[int] = None) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/shell/wait",
            json={"id": session_id, "seconds": seconds}
        )
        return ToolResult(**response.json())

    async def write_to_process(self, session_id: str, input_text: str, press_enter: bool = True) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/shell/write",
            json={"id": session_id, "input": input_text, "press_enter": press_enter}
        )
        return ToolResult(**response.json())

    async def kill_process(self, session_id: str) -> ToolResult:
        response = await self.client.post(f"{self.base_url}/api/v1/shell/kill", json={"id": session_id})
        return ToolResult(**response.json())

    async def file_write(self, file: str, content: str, append: bool = False, 
                        leading_newline: bool = False, trailing_newline: bool = False, 
                        sudo: bool = False) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/file/write",
            json={"file": file, "content": content, "append": append, 
                  "leading_newline": leading_newline, "trailing_newline": trailing_newline, "sudo": sudo}
        )
        return ToolResult(**response.json())

    async def file_read(self, file: str, start_line: int = None, end_line: int = None, sudo: bool = False) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/file/read",
            json={"file": file, "start_line": start_line, "end_line": end_line, "sudo": sudo}
        )
        return ToolResult(**response.json())
        
    async def file_exists(self, path: str) -> ToolResult:
        response = await self.client.post(f"{self.base_url}/api/v1/file/exists", json={"path": path})
        return ToolResult(**response.json())
        
    async def file_delete(self, path: str) -> ToolResult:
        response = await self.client.post(f"{self.base_url}/api/v1/file/delete", json={"path": path})
        return ToolResult(**response.json())
        
    async def file_list(self, path: str) -> ToolResult:
        response = await self.client.post(f"{self.base_url}/api/v1/file/list", json={"path": path})
        return ToolResult(**response.json())

    async def file_replace(self, file: str, old_str: str, new_str: str, sudo: bool = False) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/file/replace",
            json={"file": file, "old_str": old_str, "new_str": new_str, "sudo": sudo}
        )
        return ToolResult(**response.json())

    async def file_search(self, file: str, regex: str, sudo: bool = False) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/file/search",
            json={"file": file, "regex": regex, "sudo": sudo}
        )
        return ToolResult(**response.json())

    async def file_find(self, path: str, glob_pattern: str) -> ToolResult:
        response = await self.client.post(
            f"{self.base_url}/api/v1/file/find",
            json={"path": path, "glob": glob_pattern}
        )
        return ToolResult(**response.json())

    async def file_upload(self, file_data: BinaryIO, path: str, filename: str = None) -> ToolResult:
        files = {"file": (filename or "upload", file_data, "application/octet-stream")}
        data = {"path": path}
        response = await self.client.post(f"{self.base_url}/api/v1/file/upload", files=files, data=data)
        return ToolResult(**response.json())

    async def file_download(self, path: str) -> BinaryIO:
        response = await self.client.get(f"{self.base_url}/api/v1/file/download", params={"path": path})
        response.raise_for_status()
        return io.BytesIO(response.content)
    
    async def destroy(self) -> bool:
        """Destroy Fly.io Machine sandbox"""
        try:
            if self.client:
                await self.client.aclose()
            
            settings = get_settings()
            async with await FlySandbox._get_fly_client() as fly_client:
                response = await fly_client.delete(
                    f"/apps/{settings.fly_sandbox_app}/machines/{self.machine_id}?force=true"
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Failed to destroy Fly sandbox: {str(e)}")
            return False
    
    async def get_browser(self) -> Browser:
        settings = get_settings()
        engine = (settings.browser_engine or "playwright").lower().strip()
        if engine == "browser_use":
            return BrowserUseBrowser(self.cdp_url)
        return PlaywrightBrowser(self.cdp_url)

    @classmethod
    async def create(cls) -> Sandbox:
        return await FlySandbox._create_task()
    
    @classmethod
    @alru_cache(maxsize=128, typed=True)
    async def get(cls, id: str) -> Sandbox:
        settings = get_settings()
        async with await FlySandbox._get_fly_client() as fly_client:
            response = await fly_client.get(f"/apps/{settings.fly_sandbox_app}/machines/{id}")
            response.raise_for_status()
            data = response.json()
            return FlySandbox(machine_id=id, ip=data["private_ip"])
