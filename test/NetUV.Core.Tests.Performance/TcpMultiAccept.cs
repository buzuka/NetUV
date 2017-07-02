// Copyright (c) Johnny Z. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace NetUV.Core.Tests.Performance
{
    using System;
    using System.Text;
    using System.Threading;
    using NetUV.Core.Buffers;
    using NetUV.Core.Handles;

    sealed class TcpMultiAccept : IDisposable
    {
        static readonly string IpcPipeName =  TestHelper.GetPipeName("TEST_PIPENAME");
        const int NumConnects = (250 * 1000);

        readonly int serverCount;
        readonly int clientCount;

        sealed class ServerContext
        {
            readonly SemaphoreSlim semaphore;
            readonly Thread thread;

            Loop loop;
            Async asyncHandle;

            public ServerContext()
            {
                this.Connects = 0;
                this.semaphore = new SemaphoreSlim(0, 1);
                this.thread = new Thread(Start);
                this.thread.Start(this);
            }

            public ServerStream ServerHandle { get; set; }

            public int Connects { get; private set; }

            public void Release()
            {
                this.semaphore.Release(1);
            }

            public void Wait()
            {
                this.semaphore.Wait();
            }

            static void Start(object state)
            {
                var ctx = (ServerContext)state;

                var loop = new Loop();
                ctx.loop = loop;
                ctx.asyncHandle = loop.CreateAsync(OnAsync);
                ctx.asyncHandle.UserToken = ctx;
                ctx.asyncHandle.RemoveReference();

                // Wait until the main thread is ready.
                ctx.semaphore.Wait();
                ctx.GetListenHandle();
                ctx.Release();

                // Now start the actual benchmark.
                ctx.ServerHandle.StreamListen(ctx.OnServerConnection);
                ctx.loop.RunDefault();
                ctx.loop.Dispose();
            }

            void OnServerConnection(StreamHandle handle, Exception exception)
            {
                if (exception != null)
                {
                    Console.WriteLine($"Tcp multi accept : server connection error {exception}");
                    handle.CloseHandle();
                }
                else
                {
                    this.Connects++;
                    handle.OnRead(OnAccept, OnError);
                }
            }

            static void OnAccept(StreamHandle handle, ReadableBuffer readableBuffer)
            {
                // NOP
            }

            static void OnError(StreamHandle handle, Exception exception) =>
                Console.WriteLine($"Tcp multi accept : read error {exception}.");

            void GetListenHandle()
            {
                Pipe ipcPipe = this.loop.CreatePipe(true);
                var context = new IpcClientContext(ipcPipe);
                ipcPipe.UserToken = context;
                ipcPipe.ConnectTo(IpcPipeName, OnIpcPipeConnected);

                this.loop.RunDefault();
            }

            static void OnIpcPipeConnected(Pipe handle, Exception exception)
            {
                if (exception != null)
                {
                    Console.WriteLine($"Tcp multi accept : server ipc pipe connection error {exception}");
                    handle.CloseHandle();
                }
                else
                {
                    handle.OnRead(OnIpcPipeAccept, OnError);
                }
            }

            static void OnIpcPipeAccept(Pipe handle, ReadableBuffer readableBuffer)
            {
                var context = (IpcClientContext)handle.UserToken;
                context.ServerHandle = handle.CreatePendingType();
                handle.CloseHandle();
            }

            static void OnAsync(Async handle)
            {
                var ctx = (ServerContext)handle.UserToken;
                ctx.ServerHandle.CloseHandle();
                ctx.asyncHandle.CloseHandle();
            }
        }

        sealed class IpcClientContext
        {
            static readonly byte[] Scratch = new byte[16];
            readonly Pipe ipcPipe;

            public StreamHandle ServerHandle { get; set; }

            public IpcClientContext(Pipe ipcPipe)
            {
                this.ipcPipe = ipcPipe;
            }
        }

        sealed class IpcServerContext : IDisposable
        {
            readonly WritableBuffer buf;
            Loop loop;
            Tcp serverHandle;
            Pipe ipcPipe;
            int connects;

            public IpcServerContext(int connects)
            {
                this.loop = new Loop();
                this.serverHandle = this.loop
                    .CreateTcp()
                    .Bind(TestHelper.LoopbackEndPoint);

                this.ipcPipe = this.loop
                    .CreatePipe(true)
                    .Bind(IpcPipeName)
                    .Listen(this.OnPipeConnection);

                this.connects = connects;
                byte[] content = Encoding.UTF8.GetBytes("PING");
                this.buf = WritableBuffer.From(content);
            }

            public void Run()
            {
                this.loop.RunDefault();
                this.serverHandle.CloseHandle();
                this.loop.RunDefault();
            }

            void OnPipeConnection(Pipe handle, Exception exception)
            {
                handle.QueueWriteStream(this.buf, this.serverHandle, OnWriteCompleted);

                this.connects--;
                if (this.connects == 0)
                {
                    this.ipcPipe.CloseHandle();
                }
            }

            static void OnWriteCompleted(StreamHandle handle, Exception exception)
            {
                if (exception != null)
                {
                    Console.WriteLine($"Tcp multi accept : ipc server write error {exception}");
                }

                handle.CloseHandle();
            }

            public void Dispose()
            {
                this.serverHandle?.CloseHandle();
                this.serverHandle = null;

                this.ipcPipe?.CloseHandle();
                this.ipcPipe = null;

                this.loop?.Dispose();
                this.loop = null;
            }
        }

        ServerContext[] servers;
        IpcServerContext ipcServer;

        public TcpMultiAccept(int serverCount, int clientCount)
        {
            this.serverCount = serverCount;
            this.clientCount = clientCount;
        }

        public void Run()
        {
            this.servers = new ServerContext[this.serverCount];
            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i] = new ServerContext();
            }

            this.SendListenHandles();


        }

        void SendListenHandles()
        {
            this.ipcServer = new IpcServerContext(this.serverCount);

            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i].Release();
            }

            this.ipcServer.Run();

            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i].Wait();
            }
        }

        public void Dispose()
        {
            this.ipcServer?.Dispose();
            this.ipcServer = null;
        }
    }
}
