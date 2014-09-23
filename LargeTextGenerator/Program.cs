using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LargeTextGenerator
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Load all English words
            Console.WriteLine("Loading English words");
            List<String> allEnglishWords = new List<String>(
                File.ReadAllLines(Path .Combine(Environment.CurrentDirectory, "EnglishWords.txt")));

            // Get input
            Console.WriteLine("[files count] [files size (MB)]");
            String[] tokens = Console.ReadLine().Split(' ');
            int count = int.Parse(tokens[0]);
            int size = 1000000 * int.Parse(tokens[1]);

            // Gen the files
            List<String> allFilesPaths = new List<String>();
            for (int i = 0; i < count; i++)
            {
                String filePath = Path.Combine(Environment.CurrentDirectory, "LargeFile" + (i + 1).ToString() + ".txt");
                allFilesPaths.Add(filePath);
                Console.WriteLine("Creating file " + filePath);
                GenFile(filePath, size, allEnglishWords);
            }

            // Write out a list of all files made
            File.WriteAllLines(Path.Combine(Environment.CurrentDirectory, "FilesPaths.txt"), allFilesPaths);

            Console.WriteLine("Done.");
        }

        private static void GenFile(String path, int size, List<String> allEnglishWords)
        {
            Random random = new Random();

            using (StreamWriter outfile = new StreamWriter(path))
            {
                for (int thisSize = 0; thisSize < size; /* NA */)
                {
                    String word = allEnglishWords[random.Next(0, allEnglishWords.Count - 1)];
                    outfile.WriteLine(word);
                    thisSize += word.Length;
                }
            }

        }
    }
}
