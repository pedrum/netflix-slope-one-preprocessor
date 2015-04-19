using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;

namespace import_csharp
{
    class Program
    {
        private static string TRAINING_SET_PATH = @"C:\Projects\Netflix\training_set\";
        private static string OUTPUT_PATH = @"C:\Projects\Netflix\average_diffs\";
        private static int MAX_THREAD_COUNT = 4;
        private static Dictionary<short, Dictionary<int, short>> Ratings = new Dictionary<short, Dictionary<int, short>>();
        private static ConcurrentQueue<short> Movies = new ConcurrentQueue<short>();

        private static int fileLimit = 17770;

        private static void ImportRatings()
        {
            string[] fileList = File.ReadAllLines(TRAINING_SET_PATH + "files.txt");
            int fileCounter = 1;
            foreach (string mvFilePath in fileList)
            {
                if (fileCounter++ > fileLimit) break;

                Console.WriteLine("Merging: " + mvFilePath);
                string[] mvLines = File.ReadAllLines(TRAINING_SET_PATH + mvFilePath);
                short movieId = Convert.ToInt16(mvLines[0].Substring(0, mvLines[0].Length - 1));
                Dictionary<int, short> itemRatings = new Dictionary<int, short>();
                Movies.Enqueue(movieId);

                for (int i = 1; i < mvLines.Length; ++i)
                {
                    string[] mvLine = mvLines[i].Split(',');
                    itemRatings[Convert.ToInt32(mvLine[0])] = Convert.ToInt16(mvLine[1]);
                }

                Ratings[movieId] = itemRatings;
            }
        }

        private static void AverageDiffsWorker()
        {
            short movieId;
            List<string> outputLines = new List<string>();

            while (Movies.TryDequeue(out movieId))
            {
                Console.WriteLine(Thread.CurrentThread.Name + ": Starting MovieID(" + movieId + ")");
                outputLines.Clear();
                outputLines.Add(movieId + ":");

                short otherMovieId = movieId;
                ++otherMovieId;
                for (; otherMovieId <= Ratings.Count; ++otherMovieId)
                {
                    int totalDiff = 0, totalUsers = 0;
                    float averageDiff = 0f;

                    foreach (int userId in Ratings[movieId].Keys.Intersect(Ratings[otherMovieId].Keys))
                    {
                        totalDiff += (Ratings[movieId][userId] - Ratings[otherMovieId][userId]);
                        totalUsers++;
                    }

                    averageDiff = ((float)totalDiff) / totalUsers;
                    outputLines.Add(otherMovieId + "," + averageDiff);
                }

                System.IO.File.WriteAllLines(OUTPUT_PATH + movieId + ".txt", outputLines);
                Console.WriteLine(Thread.CurrentThread.Name + ": Finished MovieID(" + movieId + ")");
            }
        }

        private static void MakeAverageDiffsConcurrent()
        {
            for (int i = 1; i <= MAX_THREAD_COUNT; ++i)
            {
                ThreadStart threadDelegate = new ThreadStart(AverageDiffsWorker);
                Thread newThread = new Thread(threadDelegate);
                newThread.Name = "Thread " + i;
                newThread.Start();
            }
        }

        private static void MergeFiles()
        {
            string[] fileList = File.ReadAllLines(OUTPUT_PATH + "files.txt");
            
            int fileCounter = 1;
            foreach (string mvFilePath in fileList.OrderBy(x=> Convert.ToInt16(x.Split('.')[0])  ))
            {
                Console.WriteLine("Merging: " + mvFilePath);
                string[] mvLines = File.ReadAllLines(OUTPUT_PATH + mvFilePath);

                if (fileCounter++ == 1)
                    System.IO.File.WriteAllLines(OUTPUT_PATH + "averegeDiffs.txt", mvLines);
                else
                    System.IO.File.AppendAllLines(OUTPUT_PATH + "averegeDiffs.txt", mvLines);
            }
        }

        static void Main(string[] args)
        {
            ImportRatings();
            MakeAverageDiffsConcurrent();
            //MergeFiles();
        }
    }
}
