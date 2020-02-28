using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GarbageGen
{
    class Program
    {
        static void Main(string[] args)
        {
            if(args.Length != 6)
            {
                Console.WriteLine("Bad args");
            }
            GarbageGenerator ggen = new GarbageGenerator();
            ggen.Start(args);
        }
    }
}
