using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace Launcher
{
    //REST API
    //https://mesos.github.io/chronos/docs/api.html#manually-starting-a-job
    class Program
    {
        class JobData
        {
            public string JobId;
            public int A, C, G, T;
        }

        const int MIN_SIZE = 70000000;
        const int MAX_SIZE = 90000000;
        const string EXECUTOR_NAME = "demo.exe";
        static int job_num;

        static List<JobData> jobs = new List<JobData>();



        static Sider.RedisClient redis = null;
        //  static CSRedis.RedisClient redis = new CSRedis.RedisClient("10.0.0.90");

        static T ReadLine<T>(string msg)
        {
            while (true)
            {
                try
                {
                    Console.Write(msg);
                    T ret = (T)Convert.ChangeType(Console.ReadLine(), typeof(T));
                    return ret;
                }
                catch
                {
                    Print("Input error! >_<");
                }

            }
        }
        static void Print(string str)
        {
            Console.WriteLine(str);
        }
        static void Main(string[] args)
        {
            Console.WriteLine("YHGenomics Inc.");
            Console.WriteLine("Distribution System Demo");

            Print(">>>Connecting to Redis server");
            redis = new Sider.RedisClient("10.0.0.90");
            //redis.Connect(999999);

            job_num = ReadLine<int>("Job num: ");
            Print(">>>" + job_num + " Jobs Comfirmed");

            Print(">>>Creating Data");
            CreateData();

            Print(">>>Launching Jobs");
            LaunchJob();

            while (!UpdateStatus()) ;

            CheckResult();

            Print(">>>Demo Finished");

            while (true)
            {
                Console.ReadLine();
            }
        }
        static void CreateData()
        {
            for (int i = 0; i < job_num; i++)
            {
                JobData job = new JobData();
                try
                {
                    job.JobId = Guid.NewGuid().ToString().Replace("-", "");
                    var fileName = ("/wwwroot/" + job.JobId + ".data");
                    StringBuilder data = new StringBuilder();
                    Random rnd = new Random();
                    int size = rnd.Next(MIN_SIZE, MAX_SIZE);
                    for (int j = 0; j < size; j++)
                    {
                        int snp = rnd.Next(0, 100) % 4;
                        switch (snp)
                        {
                            case 0:
                                data.Append("A");
                                job.A++;
                                break;
                            case 1:
                                data.Append("C");
                                job.C++;
                                break;
                            case 2:
                                data.Append("G");
                                job.G++;
                                break;
                            case 3:
                                data.Append("T");
                                job.T++;
                                break;
                        }
                    }
                    jobs.Add(job);
                    System.IO.File.WriteAllText(fileName, data.ToString());
                    Print("Data " + job.JobId + " Finished...");
                }
                catch
                {
                    Print(">>>Creating Job " + job.JobId + " Error!");
                }
            }
        }
        static void LaunchJob()
        {

            for (int i = 0; i < job_num; i++)
            {
                JobData job = jobs[i];
                try
                {
                    WebClient wc = new WebClient();
                    wc.Encoding = Encoding.UTF8;
                    wc.Headers[HttpRequestHeader.ContentType] = "application/json";

                    StringBuilder command = new StringBuilder();
                    command.Append("{");
                    command.Append("\"name\" : \"" + job.JobId + "\" , ");
                    command.Append("\"schedule\" : \"R1/" + DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ssZ/PT2S") + "\" ,");
                    command.Append("\"scheduleTimeZone\":\"\",");
                    command.Append("\"async\": false ,");
                    command.Append("\"command\" : \"chmod 777 "+ EXECUTOR_NAME + " && ./" + EXECUTOR_NAME + " " + job.JobId + "\" , ");
                    command.Append("\"owner\" : \"shuboyang@yhgenomics.com\" , ");
                    command.Append("\"uris\" : [\"http://10.0.0.90/" + EXECUTOR_NAME + "\",\"http://10.0.0.90/" + job.JobId + ".data\" , \"http://10.0.0.90/Sider.dll\" , \"http://10.0.0.90/System.IO.dll\" , \"http://10.0.0.90/System.Runtime.dll\" , \"http://10.0.0.90/System.Threading.Tasks.dll\" ] , ");
                    command.Append("\"runAsUser\" : \"root\" , ");
                    command.Append("\"environmentVariables\" : [{ \"name\" : \"id\" , \"value\" : \"" + job.JobId + "\"}] , ");
                    command.Append("\"cpus\" : 1.0");
                    //command.Append("\"mem\" : 10240");
                    command.Append("}");
                    string result = wc.UploadString("http://10.0.0.13:4400/scheduler/iso8601", "POST", (command.ToString()));
                   
                }
                catch (System.Net.WebException eee)
                {
                    StreamReader reader = new StreamReader(eee.Response.GetResponseStream());
                    Print(">>>Create Job " + job.JobId + " Error: " + eee.Message + " Data: " + reader.ReadToEnd());
                }
            }

            for (int i = 0; i < job_num; i++)
            {
                JobData job = jobs[i];
                try
                {
                    //WebClient wc = new WebClient();
                    //wc.Encoding = Encoding.UTF8;
                    //var StreamWriter = new StreamWriter(wc.OpenWrite("http://10.0.0.13:4400/scheduler/job/" + job.JobId, "PUT"));
                    //StreamWriter.Write(" ");
                    //StreamWriter.Close();
                    ProcessStartInfo info = new ProcessStartInfo();
                    info.FileName = "curl";
                    info.Arguments = "-L -X PUT http://10.0.0.13:4400/scheduler/job/" + job.JobId;
                    var p = Process.Start(info);
                    p.WaitForExit();
                    //string result = wc.UploadString("http://10.0.0.13:4400/scheduler/job/"+job.JobId, "PUT");
                    Print(">>>Launch Job " + job.JobId + " Successed ");
                }
                catch (System.Net.WebException eee)
                {
                    StreamReader reader = new StreamReader(eee.Response.GetResponseStream());
                    Print(">>>Launch Job " + job.JobId + " Error: " + eee.Message + " Data: " + reader.ReadToEnd());
                }
            }
        }
        static bool UpdateStatus()
        {
            while (true)
            {
                bool alldone = true;
                foreach (var job in jobs)
                {
                    var progress = redis.Get(job.JobId + "_progress");
                    if (progress == null)
                    {
                        progress = "0";
                    }

                    Print(">>>Job " + job.JobId + " Processed " + progress + "%");
                    if (!progress.Equals("100"))
                    {
                        alldone = false;
                    }
                }

                Print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

                if (alldone)
                {
                    return true;
                }

                Thread.Sleep(1000);
            }
            return false;
        }

        static void CheckResult()
        {
            Print(">>>Jobs all done!");
            foreach (var job in jobs)
            {
                var a = long.Parse(redis.Get(job.JobId + "_A"));
                var t = long.Parse(redis.Get(job.JobId + "_T"));
                var c = long.Parse(redis.Get(job.JobId + "_C"));
                var g = long.Parse(redis.Get(job.JobId + "_G"));

                Print(string.Format(
                    ">>>Job {0} A:{1}/{2} T:{3}/{4} C:{5}/{6} G:{7}/{8}",
                    job.JobId,
                    a, job.A,
                    t, job.T,
                    c, job.C,
                    g, job.G));
            }
        }
    }



}
