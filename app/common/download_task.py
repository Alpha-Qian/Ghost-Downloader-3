import asyncio
import struct
import sys
import time
from asyncio import Task,Event
from pathlib import Path
from threading import Thread
from contextlib import asynccontextmanager, contextmanager

import curl_cffi
from PySide6.QtCore import QThread, Signal
from loguru import logger

from app.common.config import cfg
from app.common.methods import getProxy, getReadableSize, getLinkInfo, createSparseFile, getLinkInfoFromResponse


class DownloadWorker:
    """只能出卖劳动力的最底层工作者"""

    def __init__(self, start, progress, end, client: curl_cffi.AsyncSession):
        self.startPos = start
        self.progress = progress
        self.endPos = end

        self.client = client


class MutiThreadContext:
    """多线程句柄，如果DownloadTask的方法需要使用多线程，则需要将该类作为参数传入"""
    def __init__(self, fileSize):
        self.workers: list[DownloadWorker] = []
        self.taskgroup = asyncio.TaskGroup()
        self.created:int = 0
        self.done:int = 0
        self.fileSize:int = fileSize

    @property
    def running_task_num(self):
        return self.created - self.done


class DownloadTask(QThread):
    """Task Manager
    self.fileSize == -1 表示自动获取; == 0 表示不能并行下载; else 表示正常"""

    taskInited = Signal(bool)  # 线程初始化成功, 并传递是否支持并行下载的信息
    # processChange = Signal(str)  # 目前进度 且因为C++ int最大值仅支持到2^31 PyQt又没有Qint类 故只能使用str代替
    workerInfoChanged = Signal(list)  # 目前进度 v3.2版本引进了分段式进度条
    speedChanged = Signal(
        int
    )  # 平均速度 因为 autoSpeedUp 功能需要实时计算平均速度 v3.4.4 起移入后端计算速度, 每秒速度可能超过 2^31 Bytes 吗？
    taskFinished = Signal()  # 内置信号的不好用
    gotWrong = Signal(str)  # 😭 我出问题了

    def __init__(
        self,
        url,
        headers,
        preTaskNum: int = 8,
        filePath: str = None,
        fileName: str = None,
        autoSpeedUp: bool = False,
        fileSize: int = -1,
        resumeInfo: dict | None = None,
        parent=None,
    ):
        super().__init__(parent)

        self.progress = 0
        self.url = url
        self.headers = headers
        self.fileName = fileName
        self.filePath = filePath
        self.preBlockNum = preTaskNum
        self.autoSpeedUp = autoSpeedUp
        self.fileSize = fileSize
        self.ableToParallelDownload: bool

        self.historySpeed = [0] * 10  # 历史速度 10 秒内的平均速度
        self.inited = Event()
        self.firstWorkerCountinue = Event()
        self.resumeInfo = resumeInfo

        proxy = getProxy()

        self.client = curl_cffi.AsyncSession(
            headers=headers,
            verify=cfg.SSLVerify.value,
            proxy=proxy,
            max_clients=256,
            trust_env=False,
            allow_redirects=True,
            impersonate="chrome",
            http_version="v3",
        )


    def __reassignWorker(self, context: MutiThreadContext):

        # 找到剩余进度最多的线程
        maxRemainder = 0
        maxRemainderWorkerProcess = 0
        maxRemainderWorkerEnd = 0
        maxRemainderWorker: DownloadWorker = None

        for i in context.workers:
            if (
                i.endPos - i.progress
            ) > maxRemainder:  # 其实逻辑有一点问题, 但是影响不大
                maxRemainderWorkerProcess = i.progress
                maxRemainderWorkerEnd = i.endPos
                maxRemainder = maxRemainderWorkerEnd - maxRemainderWorkerProcess
                maxRemainderWorker = i

        if (
            maxRemainderWorker and maxRemainder > cfg.maxReassignSize.value * 1048576
        ):  # 转换成 MB
            # 平均分配工作量
            baseShare = maxRemainder // 2
            remainder = maxRemainder % 2

            maxRemainderWorker.endPos = (
                maxRemainderWorkerProcess + baseShare + remainder
            )  # 直接修改好像也不会怎么样

            # 安配新的工人
            startPos = maxRemainderWorkerProcess + baseShare + remainder + 1

            newWorker = DownloadWorker(
                startPos, startPos, maxRemainderWorkerEnd, self.client
            )

            context.taskgroup.create_task(self.__handleWorker(newWorker, context))
            context.workers.insert(context.workers.index(maxRemainderWorker) + 1, newWorker)

            logger.info(
                f"Task{self.fileName} 分配新线程成功, 剩余量：{getReadableSize(maxRemainder)}，修改后的EndPos：{maxRemainderWorker.endPos}，新线程：{newWorker}，新线程的StartPos：{startPos}"
            )

        else:
            logger.info(
                f"Task{self.fileName} 欲分配新线程失败, 剩余量小于最小分块大小, 剩余量：{getReadableSize(maxRemainder)}"
            )

    def __calcDivisionalRange(self, context: MutiThreadContext):
        step = self.fileSize // self.preBlockNum  # 每块大小
        arr = list(range(0, self.fileSize, step))

        # 否则线程数可能会不按预期地少一个
        if self.fileSize % self.preBlockNum == 0:
            arr.append(self.fileSize)

        stepList = []

        for i in range(len(arr) - 1):  #

            startPos, endPos = arr[i], arr[i + 1] - 1
            stepList.append([startPos, endPos])

        stepList[-1][-1] = self.fileSize - 1  # 修正

        return stepList

    def __initTask(self, response):
        """获取链接信息并初始化线程"""
        try:
            if self.fileSize == -1 or not self.fileName:
                self.url, self.fileName, self.fileSize = getLinkInfoFromResponse(response)

            if self.fileSize:
                self.ableToParallelDownload = True
            else:
                self.ableToParallelDownload = False  # 处理无法并行下载的情况

            # 获取文件路径
            if not self.filePath and Path(self.filePath).is_dir() == False:
                self.filePath = Path.cwd()

            else:
                self.filePath = Path(self.filePath)
                if not self.filePath.exists():
                    self.filePath.mkdir()

            # 检验文件合法性并自动重命名
            if sys.platform == "win32":
                self.fileName = "".join(
                    [i for i in self.fileName if i not in r'\/:*?"<>|']
                )  # 去除Windows系统不允许的字符
            if len(self.fileName) > 255:
                self.fileName = self.fileName[:255]

            filePath = Path(f"{self.filePath}/{self.fileName}")

            if not filePath.exists():
                filePath.touch()
                try:
                    createSparseFile(filePath)
                except Exception as e:
                    logger.warning("创建稀疏文件失败", repr(e))

            # 任务初始化完成
            if self.ableToParallelDownload:
                self.taskInited.emit(True)
            else:
                self.taskInited.emit(False)
                self.preBlockNum = 1

        except Exception as e:  # 重试也没用
            self.gotWrong.emit(repr(e))

    def __loadWorkers(self, context: MutiThreadContext):

        # 如果 .ghd 文件存在，读取并解析二进制数据
        filePath = Path(f"{self.filePath}/{self.fileName}.ghd")
        if filePath.exists():
            try:
                with open(filePath, "rb") as f:
                    while True:
                        data = f.read(
                            24
                        )  # 每个 worker 有 3 个 64 位的无符号整数，共 24 字节

                        if not data:
                            break

                        start, process, end = struct.unpack("<QQQ", data)
                        context.workers.append(
                            DownloadWorker(start, process, end, self.client)
                        )

            except Exception as e:
                logger.error(f"Failed to load workers: {e}")
                stepList = self.__calcDivisionalRange(context)

                for i in range(self.preBlockNum):
                    context.workers.append(
                        DownloadWorker(
                            stepList[i][0], stepList[i][0], stepList[i][1], self.client
                        )
                    )
        else:
            stepList = self.__calcDivisionalRange()

            for i in range(self.preBlockNum):
                self.workers.append(
                    DownloadWorker(
                        stepList[i][0], stepList[i][0], stepList[i][1], self.client
                    )
                )

    # 多线程主下载逻辑
    async def __handleWorker(self, worker: DownloadWorker, context: MutiThreadContext):
        logger.debug(
            f"{self.fileName} task is launching the worker {worker.startPos}-{worker.endPos}..."
        )
        if worker.progress < worker.endPos:  # 因为可能会创建空线程
            finished = False
            while not finished:
                try:
                    response_context = self.get_partical_response(worker)
                    await self._write_in_file(response_context, worker)

                    finished = True

                except Exception as e:
                    logger.info(
                        f"Task: {self.fileName}, Thread {worker} is reconnecting to the server, Error: {repr(e)}"
                    )

                    self.gotWrong.emit(repr(e))

                    await asyncio.sleep(5)

            worker.progress = worker.endPos

    async def __handleFirstWorker(self, worker: DownloadWorker, response: curl_cffi.Response):
        if worker.progress < worker.endPos:  # 因为可能会创建空线程
            finished = False
            while not finished:
                try:
                    response = self.get_first_worker_response()
                    await self._write_in_file(response, worker)

                except Exception as e:
                    logger.info(
                        f"Task: {self.fileName}, Thread {worker} is reconnecting to the server, Error: {repr(e)}"
                    )

                    self.gotWrong.emit(repr(e))

                    await asyncio.sleep(5)

            worker.progress = worker.endPos

    async def _write_in_file(self, response, worker: DownloadWorker):
        try:
            async for chunk in response.aiter_content():
                if worker.endPos <= worker.progress:
                    break
                if chunk:
                    self.file.seek(worker.progress)
                    self.file.write(chunk)
                    chunkSize = len(chunk)
                    worker.progress += chunkSize
                    cfg.globalSpeed += chunkSize
                    if cfg.speedLimitation.value:
                        if cfg.globalSpeed >= cfg.speedLimitation.value:
                            time.sleep(1)
            if worker.progress >= worker.endPos:
                worker.progress = worker.endPos
        finally:
            await response.aclose()

    async def __supervisor(self, context: MutiThreadContext):
        """实时统计进度并写入历史记录文件"""
        LastProgress = (
            0  # 可能会出现unbound error，所以将LastProgress提取为函数全局变量
        )

        for i in context.workers:
            self.progress += i.progress - i.startPos + 1
            LastProgress = self.progress
        if self.autoSpeedUp:
            # 初始化变量
            maxSpeedPerConnect = 1  # 防止除以 0
            additionalTaskNum = context.running_task_num  # 最初为计算每个线程的平均速度
            formerAvgSpeed = 0  # 提速之前的平均速度
            duringTime = 0  # 计算平均速度的时间间隔, 为 10 秒
            _ = 0
        async with asyncio.TaskGroup() as self.taskGroup:
            while not self.progress == self.fileSize:

                info = []
                # 记录每块信息
                self.ghdFile.seek(0)
                self.progress = 0

                for i in context.workers:
                    info.append(
                        {"start": i.startPos, "progress": i.progress, "end": i.endPos}
                    )

                    self.progress += i.progress - i.startPos + 1

                    # 保存 workers 信息为二进制格式
                    data = struct.pack("<QQQ", i.startPos, i.progress, i.endPos)
                    self.ghdFile.write(data)

                self.ghdFile.flush()
                self.ghdFile.truncate()

                self.workerInfoChanged.emit(info)

                # 计算速度
                speed = self.progress - LastProgress
                # print(f"speed: {speed}, progress: {self.progress}, LastProgress: {LastProgress}")
                LastProgress = self.progress
                self.historySpeed.pop(0)
                self.historySpeed.append(speed)
                avgSpeed = sum(self.historySpeed) / 10

                self.speedChanged.emit(avgSpeed)

                # print(f"avgSpeed: {avgSpeed}, historySpeed: {self.historySpeed}")

                if self.autoSpeedUp:
                    if duringTime < 10:
                        duringTime += 1
                    else:
                        duringTime = 0

                        speedPerConnect = avgSpeed / context.running_task_num
                        if speedPerConnect > maxSpeedPerConnect:
                            maxSpeedPerConnect = speedPerConnect
                            _ = (
                                0.9 * maxSpeedPerConnect * additionalTaskNum
                            ) + formerAvgSpeed

                        # if maxSpeedPerConnect <= 1:
                        #     await asyncio.sleep(1)
                        #     continue

                        # logger.debug(f"当前效率: {(avgSpeed - formerAvgSpeed) / additionalTaskNum / maxSpeedPerConnect}, speed: {speed}, formerAvgSpeed: {formerAvgSpeed}, additionalTaskNum: {additionalTaskNum}, maxSpeedPerConnect: {maxSpeedPerConnect}")

                        # 原公式：(avgSpeed - formerAvgSpeed) / additionalTaskNum / maxSpeedPerConnect >= 0.85
                        # 然后将不等号左边的计算全部移到右边
                        if avgSpeed >= _:
                            #  新增加线程的效率 >= 0.85 时，新增线程
                            # logger.debug(f'自动提速增加新线程, 当前效率: {(avgSpeed - formerAvgSpeed) / additionalTaskNum / maxSpeedPerConnect}')
                            formerAvgSpeed = avgSpeed
                            additionalTaskNum = 4
                            _ = (
                                0.85 * maxSpeedPerConnect * additionalTaskNum
                            ) + formerAvgSpeed

                            if context.running_task_num < 253:
                                for i in range(4):
                                    self.__reassignWorker(context)  # 新增线程

                await asyncio.sleep(1)

    async def __supervisorSingleThread(self):
        while (
            not self.ableToParallelDownload
        ):  # 实际上此时 self.ableToParallelDownload 用于记录任务是否完成
            self.progress = 0

            self.workerInfoChanged.emit([])

            # 计算速度
            speed = self.progress - LastProgress
            LastProgress = self.progress
            self.historySpeed.pop(0)
            self.historySpeed.append(speed)
            avgSpeed = sum(self.historySpeed) / 10

            self.speedChanged.emit(avgSpeed)

            await asyncio.sleep(1)

    async def __main(self):
        try:
            # 打开下载文件
            
            firstWorkerResponse = await self.get_first_worker_response()
            self.file = open(f"{self.filePath}/{self.fileName}", "rb+")
            self.__initTask(firstWorkerResponse)

            if self.ableToParallelDownload:
                #多线程部分
                # 加载分块
                context = MutiThreadContext(self.fileSize)

                self.__loadWorkers(context)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.__supervisor(context))
                    for i in context.workers:  # 启动 Worker
                        tg.create_task(self.__handleWorker(i, context))

                    self.ghdFile = open(f"{self.filePath}/{self.fileName}.ghd", "wb")
                    await self.__handleFirstWorker()#todo
            else:

                #单线程部分
                logger.debug(f"Task {self.fileName}, starting single thread...")
                singleWorker = DownloadWorker(0, 0, self.fileSize, self.client)
                superviewer = asyncio.create_task(self.__supervisorSingleThread())
                try:
                    await self.__handleFirstWorker(singleWorker)  # todo
                finally:
                    superviewer.cancel()

            
            

        except Exception as e:
            self.gotWrong.emit(repr(e))

        finally:  # 关闭
            self.client.close()
            self.file.close()
            if self.fileSize:
                self.ghdFile.close()
            else:
                logger.info(f"Task {self.fileName} finished!")
                self.taskFinished.emit()
            
            if self.progress == self.fileSize:
                try:
                    Path(f"{self.filePath}/{self.fileName}.ghd").unlink()
                except Exception as e:
                    logger.error(
                        f"Failed to delete the history file, please delete it manually. Err: {e}"
                    )
                logger.info(f"Task {self.fileName} finished!")
                self.taskFinished.emit()


    def stop(self):
        # 关闭
        self._mainTask.cancel()

    # @retry(3, 0.1)
    def run(self):

        

        # 主逻辑, 使用事件循环启动异步任务
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self._mainTask = self.loop.create_task(self.__main())
            self.loop.run_until_complete(self.__main())
        except asyncio.CancelledError as e:
            print(e)
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
    
    async def get_first_worker_response(self):
        headers = self.headers.copy()
        headers["range"] = "bytes=0-"
        rsp = await self.client.request(method="GET", url=self.url, headers=self.headers, timeout=30, stream=True)
        try:
            rsp.raise_for_status()
            if rsp.status_code == 206:
                self.ableToParallelDownload = True        #todo: getlinkinfo
                self.inited.set()
            else:
                self.ableToParallelDownload = False
                self.inited.set()
            self.__initTask()

            

    async def get_partical_response(self, worker: DownloadWorker):
        headers = self.headers.copy()
        headers["range"] = f"bytes={worker.startPos}-{worker.endPos}"
        rsp = await self.client.request(method="GET", url=self.url, headers=self.headers, timeout=30, stream=True)
        return rsp
        # try:
        #     rsp.raise_for_status()
        #     yield rsp
        # except:
        #     await rsp.aclose()
    async def foo(self):
        await asyncio.create_task(self.bar())