// Copyright (c) Johnny Z. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace NetUV.Core.Tests.Performance
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading;
    using NetUV.Core.Buffers;
    using NetUV.Core.Handles;

    sealed class TcpMultiAccept : IDisposable
    {
        static readonly string IpcPipeName =  TestHelper.GetPipeName("TEST_PIPENAME");
        const int NumConnects = 250 * 1000;

        readonly int serverCount;
        readonly int clientCount;

        sealed class ServerContext
        {
            readonly ManualResetEventSlim resetEvent;
            Thread thread;

            Loop loop;
            Async asyncHandle;
            StreamHandle serverHandle;

            public ServerContext()
            {
                this.Connects = 0;
                this.resetEvent = new ManualResetEventSlim(false);
                this.thread = new Thread(Start);
                this.thread.Start(this);
            }

            public int Connects { get; private set; }

            public void Set()
            {
                this.resetEvent.Set();
            }

            public void Wait()
            {
                this.resetEvent.Wait();
            }

            public void Send()
            {
                this.asyncHandle?.Send();
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
                ctx.resetEvent.Wait();
                ctx.GetListenHandle();
                ctx.resetEvent.Set();

                // Now start the actual benchmark.
                ((ServerStream)ctx.serverHandle).StreamListen(ctx.OnServerConnection);
                ctx.loop.RunDefault();
                ctx.loop.Dispose();

                ctx.resetEvent.Set();
                ctx.thread = null;
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
                ipcPipe.ConnectTo(IpcPipeName, this.OnIpcPipeConnected);
                this.loop.RunDefault();
            }

            void OnIpcPipeConnected(Pipe handle, Exception exception)
            {
                if (exception != null)
                {
                    Console.WriteLine($"Tcp multi accept : server ipc pipe connection error {exception}");
                    handle.CloseHandle();
                }
                else
                {
                    handle.OnRead(this.OnIpcPipeAccept, OnError);
                }
            }

            void OnIpcPipeAccept(Pipe handle, ReadableBuffer readableBuffer)
            {
                this.serverHandle = handle.CreatePendingType();
                handle.CloseHandle();
            }

            static void OnAsync(Async handle)
            {
                var ctx = (ServerContext)handle.UserToken;
                ctx.serverHandle.CloseHandle();
                ctx.asyncHandle.CloseHandle();
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

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    this.ipcPipe = this.loop
                        .CreatePipe()
                        .Bind(IpcPipeName)
                        .Listen(this.OnPipeConnection, 128, true);
                }
                else
                {
                    this.ipcPipe = this.loop
                        .CreatePipe(true)
                        .Bind(IpcPipeName)
                        .Listen(this.OnPipeConnection);
                }

                this.connects = connects;
                byte[] content = Encoding.UTF8.GetBytes("PING");
                this.buf = WritableBuffer.From(content);
            }

            public Loop LoopHandle => this.loop;

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

        sealed class ClientContext : IDisposable
        {
            int connects;
            Loop loop;
            Tcp tcp;
            Idle idle;

            public ClientContext(Loop loop, int connects)
            {
                this.tcp = loop
                    .CreateTcp()
                    .ConnectTo(TestHelper.LoopbackEndPoint, this.OnConnected);
                this.idle = loop.CreateIdle();

                this.loop = loop;
                this.connects = connects;
            }

            void OnConnected(Tcp handle, Exception exception)
            {
                if (exception != null)
                {
                    //Console.WriteLine($"Tcp multi accept : client tcp connect error {exception}");
                    handle.CloseHandle(this.OnClosed);
                }
                else
                {
                    this.connects--;
                    this.idle.Start(this.OnIdle);
                }
            }

            void OnIdle(Idle handle)
            {
                this.tcp.CloseHandle(this.OnClosed);
                this.idle.Stop();
            }

            void OnClosed(Tcp handle)
            {
                if (this.connects == 0)
                {
                    this.idle.CloseHandle();
                    return;
                }

                this.tcp = this.loop
                    .CreateTcp()
                    .ConnectTo(TestHelper.LoopbackEndPoint, this.OnConnected);
            }

            public void Dispose()
            {
                this.tcp?.CloseHandle();
                this.tcp = null;

                this.idle.CloseHandle();
                this.idle = null;

                this.loop = null;
            }
        }

        ServerContext[] servers;
        IpcServerContext ipcServer;
        ClientContext[] clients;

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

            this.clients = new ClientContext[this.clientCount];
            int connects = NumConnects / this.clientCount;

            for (int i = 0; i < this.clientCount; i++)
            {
                this.clients[i] = new ClientContext(this.ipcServer.LoopHandle, connects);
            }

            long start = this.ipcServer.LoopHandle.NowInHighResolution;
            this.ipcServer.LoopHandle.RunDefault();
            start = this.ipcServer.LoopHandle.NowInHighResolution - start;

            double time = (double)start / TestHelper.NanoSeconds;
            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i].Send();
            }

            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i].Wait();
            }

            Console.WriteLine($"Tcp multi accept : {this.serverCount} {TestHelper.Format(NumConnects/time)} accepts/sec ({NumConnects} total).");
            for (int i = 0; i < this.serverCount; i++)
            {
                Console.WriteLine($"Tcp multi accept : thread {i} {TestHelper.Format(this.servers[i].Connects / time)} accepts/sec ({this.servers[i].Connects} total) ({TestHelper.Format((this.servers[i].Connects * 100 )/ NumConnects)}) %.");
            }
        }

        void SendListenHandles()
        {
            this.ipcServer = new IpcServerContext(this.serverCount);

            for (int i = 0; i < this.serverCount; i++)
            {
                this.servers[i].Set();
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

            this.servers = null;
            this.clients = null;
        }
    }
}
