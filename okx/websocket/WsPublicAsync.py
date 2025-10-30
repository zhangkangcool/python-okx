import asyncio
import json
import logging
import time

from okx.websocket.WebSocketFactory import WebSocketFactory

logger = logging.getLogger(__name__)


class WsPublicAsync:
    def __init__(self, url, reconnect_config=None):
        """
        初始化 WebSocket 客户端
        
        Args:
            url: WebSocket URL
            reconnect_config: 重连配置字典，包含：
                - enabled: 是否启用自动重连 (默认: True)
                - max_retries: 最大重试次数 (默认: 5, -1 表示无限重试)
                - initial_delay: 初始重连延迟（秒）(默认: 1.0)
                - max_delay: 最大重连延迟（秒）(默认: 60.0)
                - backoff_factor: 指数退避因子 (默认: 2.0)
                - on_reconnect: 重连成功回调函数
                - on_disconnect: 断开连接回调函数
        """
        self.url = url
        self.subscriptions = set()
        self.callback = None
        self.loop = asyncio.get_event_loop()
        self.factory = WebSocketFactory(url)
        self.websocket = None
        
        # 重连配置
        default_config = {
            "enabled": True,
            "max_retries": 5,
            "initial_delay": 1.0,
            "max_delay": 60.0,
            "backoff_factor": 2.0,
            "on_reconnect": None,
            "on_disconnect": None
        }
        self.reconnect_config = {**default_config, **(reconnect_config or {})}
        
        # 重连状态
        self.reconnect_count = 0
        self.is_running = False
        self.subscribed_args = []  # 保存订阅参数以便重连后重新订阅

    async def connect(self):
        """建立 WebSocket 连接"""
        try:
            self.websocket = await self.factory.connect()
            logger.info(f"✅ WebSocket 连接成功: {self.url}")
            self.reconnect_count = 0  # 重置重连计数
            return True
        except Exception as e:
            logger.error(f"❌ WebSocket 连接失败: {e}")
            return False

    async def consume(self):
        """消费 WebSocket 消息（带自动重连）"""
        while self.is_running:
            try:
                if self.websocket is None:
                    logger.warning("consume called but websocket is None")
                    await asyncio.sleep(1)
                    continue
                    
                async for message in self.websocket:
                    logger.debug("Received message: {%s}", message)
                    if self.callback:
                        try:
                            self.callback(message)
                        except Exception:
                            logger.exception("Callback raised an exception")
                            
                # 如果循环正常退出（连接关闭），尝试重连
                if self.is_running:
                    logger.info("WebSocket 连接已关闭，准备重连...")
                    await self._handle_reconnect()
                    
            except Exception as e:
                # 延迟导入以避免硬依赖
                try:
                    from websockets.exceptions import ConnectionClosedError
                except Exception:
                    ConnectionClosedError = None

                # 对常见的连接关闭异常进行安静处理并记录一次性信息
                if ConnectionClosedError is not None and isinstance(e, ConnectionClosedError):
                    logger.info(f"WebSocket connection closed: {e}")
                else:
                    logger.exception("Unexpected exception in consume loop")
                
                # 通知断开连接回调
                if self.reconnect_config.get("on_disconnect"):
                    try:
                        self.reconnect_config["on_disconnect"](e)
                    except Exception:
                        logger.exception("on_disconnect callback failed")
                
                # 如果仍在运行且启用重连，尝试重连
                if self.is_running and self.reconnect_config.get("enabled"):
                    await self._handle_reconnect()
                else:
                    break

    async def _handle_reconnect(self):
        """
        处理重连逻辑（带指数退避）
        """
        if not self.reconnect_config.get("enabled"):
            logger.info("自动重连未启用，停止连接")
            self.is_running = False
            return False
        
        max_retries = self.reconnect_config.get("max_retries", 5)
        
        # -1 表示无限重试
        if max_retries != -1 and self.reconnect_count >= max_retries:
            logger.error(f"❌ 达到最大重连次数 ({max_retries})，停止重连")
            self.is_running = False
            return False
        
        self.reconnect_count += 1
        
        # 计算退避延迟
        initial_delay = self.reconnect_config.get("initial_delay", 1.0)
        max_delay = self.reconnect_config.get("max_delay", 60.0)
        backoff_factor = self.reconnect_config.get("backoff_factor", 2.0)
        
        delay = min(
            initial_delay * (backoff_factor ** (self.reconnect_count - 1)),
            max_delay
        )
        
        logger.info(f"🔄 第 {self.reconnect_count} 次重连尝试，等待 {delay:.1f} 秒...")
        await asyncio.sleep(delay)
        
        # 尝试重新连接
        try:
            # 关闭旧连接
            if self.websocket:
                try:
                    await self.factory.close()
                except:
                    pass
                self.websocket = None
            
            # 创建新工厂和连接
            self.factory = WebSocketFactory(self.url)
            success = await self.connect()
            
            if success:
                logger.info(f"✅ 重连成功 (第 {self.reconnect_count} 次尝试)")
                
                # 重新订阅之前的频道
                if self.subscribed_args:
                    logger.info(f"📡 重新订阅 {len(self.subscribed_args)} 个频道...")
                    await self._resubscribe()
                
                # 调用重连成功回调
                if self.reconnect_config.get("on_reconnect"):
                    try:
                        self.reconnect_config["on_reconnect"]()
                    except Exception:
                        logger.exception("on_reconnect callback failed")
                
                return True
            else:
                logger.warning(f"⚠️ 重连失败 (第 {self.reconnect_count} 次尝试)")
                return False
                
        except Exception as e:
            logger.error(f"❌ 重连过程出错 (第 {self.reconnect_count} 次尝试): {e}")
            return False
    
    async def _resubscribe(self):
        """重新订阅之前的频道"""
        if not self.subscribed_args:
            return
        
        try:
            if self.websocket is None:
                raise RuntimeError("WebSocket is not connected")
            
            payload = json.dumps({
                "op": "subscribe",
                "args": self.subscribed_args
            })
            await self.websocket.send(payload)
            logger.info(f"✅ 重新订阅成功")
        except Exception as e:
            logger.error(f"❌ 重新订阅失败: {e}")
            raise

    async def subscribe(self, params: list, callback):
        self.callback = callback
        # 保存订阅参数以便重连后重新订阅
        self.subscribed_args = params
        
        payload = json.dumps({
            "op": "subscribe",
            "args": params
        })
        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected")
        await self.websocket.send(payload)
        # await self.consume()

    async def unsubscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "unsubscribe",
            "args": params
        })
        logger.info(f"unsubscribe: {payload}")
        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected")
        await self.websocket.send(payload)

    async def stop(self):
        logger.info("⏹️ 停止 WebSocket...")
        self.is_running = False
        await self.factory.close()
        # 不再调用 loop.stop()，因为可能影响其他任务

    async def start(self):
        logger.info("Connecting to WebSocket...")
        self.is_running = True
        success = await self.connect()
        if not success:
            logger.error("初始连接失败")
            if self.reconnect_config.get("enabled"):
                await self._handle_reconnect()
            else:
                return
        
        # 创建后台任务并添加 done callback，以显式检索异常，避免 'Task exception was never retrieved'
        task = self.loop.create_task(self.consume())

        def _done_callback(t):
            try:
                exc = t.exception()
                if exc:
                    # 已经被记录，但在此处可以额外记录需要的上下文
                    logger.debug(f"consume task finished with exception: {exc}")
            except asyncio.CancelledError:
                logger.debug("consume task was cancelled")

        try:
            task.add_done_callback(_done_callback)
        except Exception:
            # 兼容不同 Python 版本或实现差异
            pass

    def stop_sync(self):
        self.loop.run_until_complete(self.stop())
