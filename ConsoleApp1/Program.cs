using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var incomingPublisherActions = new ConcurrentBag<PublisherActions>();
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 1111, Action = "A", Name = "P1", InsertionTime = DateTime.Now });
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 2222, Action = "A", Name = "P2", InsertionTime = DateTime.Now });
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 3333, Action = "A", Name = "P3", InsertionTime = DateTime.Now });
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 1111, Action = "B", Name = "P1", InsertionTime = DateTime.Now });
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 2222, Action = "B", Name = "P2", InsertionTime = DateTime.Now });
            incomingPublisherActions.Add(new PublisherActions { PublisherId = 3333, Action = "B", Name = "P3", InsertionTime = DateTime.Now });


            var concurrentQueueDictory = new ConcurrentDictionary<int, ConcurrentQueue<PublisherActions>>();
            var publisherGroup = incomingPublisherActions.GroupBy(pg => pg.PublisherId);
            foreach (var publisher in publisherGroup)
            {
                var publisherActions = incomingPublisherActions.Where(x => x.PublisherId == publisher.Key).OrderBy(x => x.InsertionTime);

                //check if entry in big dictionary exists.. if not create it                
                concurrentQueueDictory.TryAdd(publisher.Key, new ConcurrentQueue<PublisherActions>());

                foreach (var tempAction in publisherActions)
                {
                    concurrentQueueDictory[publisher.Key].Enqueue(tempAction);
                }

            }

            Parallel.ForEach(publisherGroup, publisher =>
            {
                var publisherName = incomingPublisherActions.Where(x => x.PublisherId == publisher.Key).Select(x => x.Name).FirstOrDefault();
                Console.WriteLine(publisherName + " thread has started");
                //do stuff here
                while (!concurrentQueueDictory[publisher.Key].IsEmpty)
                {
                    var abc = "";
                    var tempAction = new PublisherActions();
                    concurrentQueueDictory[publisher.Key].TryDequeue(out tempAction);
                    if (tempAction != null)
                    {
                        Console.WriteLine(tempAction.Name + " " + tempAction.Action + " performed");
                    }
                }

                Console.WriteLine(publisherName + " thread has ended");

            });



            Console.ReadLine();

            //group by publisherid






        }

    }
}

