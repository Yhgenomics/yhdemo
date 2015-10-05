using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.IO;

// Fast demo only Oct. 2015
// Executor to count As,Cs,Ts,Gs in any .data file
// Need to be rewrite in C++11 under a almost google style c++ code.
namespace Executor
{
    class Program
    {
        //Redis is the recommand out put 
        //static CSRedis.RedisClient redis = new CSRedis.RedisClient("10.0.0.90");
        static Sider.RedisClient redis = null;
        static string id;
        static long filelength;
        static long report;
        static long[] countboard = new long[52];

        //Read a input from console of type T
        //May not be under use. 
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

        // Out put string in console
        // Need to be put in a uniform ouput method
        static void Print(string str)
        {
            Console.WriteLine(str);
        }

        // Main 
        static void Main(string[] args)
        {
            Console.WriteLine("YHGenomics Inc.");
            Console.WriteLine("Distribution System Demo");
            Console.WriteLine("Executor begin its work");

            Print(">>>Connecting to Redis server");
            redis = new Sider.RedisClient("10.0.0.90");

            Console.WriteLine("Begin to read the data");
            ReadInData();

            Console.WriteLine(">>>Process finished");
            Console.WriteLine("Quick report:");
            Console.WriteLine("reads id: " + id + "has");
            Console.WriteLine((countboard['A' - 'A'] + countboard['a' - 'A']).ToString() + " As "
                + (countboard['C' - 'A'] + countboard['c' - 'A']).ToString() + " Cs "
                + (countboard['T' - 'A'] + countboard['t' - 'A']).ToString() + " Ts "
                + (countboard['G' - 'A'] + countboard['g' - 'A']).ToString() + " Gs ");
        

            Print(">>>Demo Finished");

           
        }

        //Read in any .data file.
        //Count As, Cs, Ts, Gs. 
        static void ReadInData()
        {
            foreach (string filefullname in
            Directory.GetFiles(Directory.GetCurrentDirectory(), "*.data"))
            {

                StreamReader reader = new StreamReader(filefullname);
                FileInfo fileinfo = new FileInfo(filefullname);
                filelength = fileinfo.Length;
                id = Path.GetFileNameWithoutExtension(filefullname);
                Console.WriteLine("Executor processs id : "+ id);

                for (int i = 0; i < 52; i++)
                {
                    countboard[i] = 0;
                }

                report = 0;
                while (!reader.EndOfStream)
                {
                    if (report % 3000000 == 0)
                    //if (report % 3 == 0)
                    {
                        ReportToRedis();
                    }
                    report++;
                    countboard[(char)reader.Read() - 'A'] += 1;
                }
                ReportToRedis();
            }
        }

        static void ReportToRedis()
        {
            //Console.WriteLine("Quick report:");
            //Console.WriteLine("reads id: " + id );
            //Console.WriteLine("process " + 100*(report+0.0)/(filelength+0.0)+"%");
            //Console.WriteLine((countboard['A' - 'A'] + countboard['a' - 'A']).ToString() + " As "
            //    + (countboard['C' - 'A'] + countboard['c' - 'A']).ToString() + " Cs "
            //    + (countboard['T' - 'A'] + countboard['t' - 'A']).ToString() + " Ts "
            //    + (countboard['G' - 'A'] + countboard['g' - 'A']).ToString() + " Gs ");

            redis.Set(id + "_progress", (100 * (report + 0.0) / (filelength + 0.0)).ToString());

            redis.Set(id + "_A", (countboard['A' - 'A'] + countboard['a' - 'A']).ToString());

            redis.Set(id + "_C", (countboard['C' - 'A'] + countboard['c' - 'A']).ToString());

            redis.Set(id + "_T", (countboard['T' - 'A'] + countboard['t' - 'A']).ToString());

            redis.Set(id + "_G", (countboard['G' - 'A'] + countboard['g' - 'A']).ToString());
        }       
    }



}
