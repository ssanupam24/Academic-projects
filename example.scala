/*Distributed implementation of Gossip and Push-Sum algorithms in Scala
  Authors: Vishal and Anupam
*/


import akka.actor._
import scala.math._
import scala.util.Random
import scala.math
import java.io._
object Gossip {

  sealed trait AlgoMessage
  case object Calculate extends AlgoMessage
  case class gossipWork(var i : Int, var senderActor : Int) extends AlgoMessage
  case class pushSumWork(var s : Double, var w : Double, var i: Int) extends AlgoMessage
  case class Initialize( var arActors : List[ActorRef], count: ActorRef, s_init : Double)
  case class counter( var  count : Int, var i : Int, var nodes: List[ActorRef],var log1: Log)
  case class counter_Pushsum( var  count : Int, var i : Int,var nodes: List[ActorRef],var log1: Log)
  case class Gossip(arActors : List[ActorRef]) extends AlgoMessage
  case class pushsum(arActors : List[ActorRef]) extends AlgoMessage
  case class start_time(start_t : Long) extends AlgoMessage
  case class msgFinal(var msg: String) extends AlgoMessage
  case class closeFileActors(var clock1: Int) extends AlgoMessage    
      
  // The worker class starts here. Here we have included the logic for Gossip and Push-Sum

  class Worker(topology : String, id : Int) extends Actor
  {
    var nodes: List[ActorRef] = Nil
    //var logger: BufferedWriter = _
    var log: Log = _
    var count  = 0
    var total = 0
    var counterRef : ActorRef = _
    var randomNumber : Int = 0
    var  status : Boolean = true
    var neighbour:List[Int] = Nil
    var s_prev : Double = 0.0
    var w_prev : Double = 0.0
    var conv : Double = pow(10,-10)
    var s : Double = 0.0
    var w : Double = 1.0
    var diff : Double = 0.0
    var clock : Int = 0
    var nodeId : Int = 0



    
   	 override def postStop() = {
    		log.closeFile(clock)
    	}


      /*Here all the values are stored in different variables for each of the 
      actors. These variables are used in the subsequent steps for further processing. */
    def receive = 
    {
      case closeFileActors(clock1) =>
           log.closeFile(clock1)
      //Initializing Actor list and neighbour list for each actor
      case Initialize(arActors : List[ActorRef], count : ActorRef, s_init : Double) =>  
      nodes = arActors
      counterRef = count
      s = s_init
      w = 1.0
      var i = (s_init - 1.0).toInt  //keeping current actor number in variable i (subtracting one as array indexes are from 0)
      nodeId = i
     
      log = new Log(i)
      var nNodes = arActors.length
    
                    //calculating neighbour list according to the topology
		    log.writeFileInfo("Neighbours are created here\n",clock = clock + 1)
                    var j:Int = 0
                    if (topology.equalsIgnoreCase("line"))
                    {
                      if(i > 0)
                        neighbour ::= i - 1
                      if(i < arActors.length -1)
                        neighbour ::=  i + 1
                    }
                 
                   
                    if ((topology.equalsIgnoreCase("2D")) || (topology.equalsIgnoreCase("Imp2D")))
                    {  
            
                        j = math.sqrt(nNodes).toInt
                
                        if (i % j == 0)
                        {
                            neighbour ::= i + 1 
                        }
                        if ((i+1) % j == 0)
                        {
                            neighbour ::= i - 1
                        }
                        if (i - j < 0)
                        {
                            neighbour ::= i + j 
                        }
                        if (i - (nNodes - j) >= 0)
                        {
                            neighbour ::= i - j
                        }
                        if (nNodes > 4) 
                        {
                            if ((i % j != 0) && ((i + 1) % j != 0))
                            {
                              neighbour ::= i - 1
                              neighbour ::= i + 1
                            }
                            if ((i - j > 0) && (i - (nNodes - j) < 0))
                            {
                              neighbour ::= i + j
                              neighbour ::= i - j
                            }
                            if (i == j) 
                            {
                              neighbour ::= i - j 
                              neighbour ::= i + j 
                            }
                        }
                    }
            
                    var random : Int = 0
                    if (topology.equalsIgnoreCase("Imp2D"))
                    {
                        var random = Random.nextInt(nNodes)
                        while(random == i) 
                        {
                        random = Random.nextInt(nNodes)
                        }
                        neighbour ::= random
                    }

      //The Gossip logic starts here.
      case gossipWork(i, senderActor) =>

            
             var j : Int = 0
           
             if(count < 10)
              {
              // log.writeFileInfo("Counter value is " + count + " ", clock = clock+1) 
               count+=1
               counterRef ! counter(count, i,nodes,log)   //Sending count values to Counter Actor     
              }
              else  if (status)   
              {
                
                status = false
		//println("Actor "+i)
		log.writeFileWarn("killed actor " + i + "\n", clock = clock + 1)
                log.writeFileInfo("Actor executed successfully\n", clock = clock+1)
		log.closeFile(clock = clock + 1)
	      context.stop(self)
              
              
	      
               
              }
          
              if(status)
              { 
		 	
	       	 clock = clock+1
                 log.writeFileInfo("Msg received in Actor " + i + " from Actor "+ senderActor + " ", clock) 
                 for (j <-1 until 3) 
                {
                  // for full topology we are not calculating neighbour list just picking any random node except the current node
                  if (topology.equalsIgnoreCase("full"))  
                  {
                    var randomNumber = Random.nextInt(nodes.length)
                        while(randomNumber == i) 
                        {
                        randomNumber = Random.nextInt(nodes.length)
                        }
                         clock = clock+1
                         log.writeFileInfo("Msg sent from Actor " + i + " to Actor "+ randomNumber + " ", clock) 
                    	nodes(randomNumber) ! gossipWork(randomNumber, nodeId)
                  } 
                  else
                  {
                    var randomNumber = Random.nextInt(neighbour.length)
                    var next = neighbour(randomNumber)
		    clock = clock+1
                    log.writeFileInfo("Msg sent from Actor " + i + " to Actor"+ neighbour(randomNumber) + " ", clock) 
                    nodes(neighbour(randomNumber))! gossipWork(next, nodeId)         
                  }
                }
             
              }
             }
		
      
   
    
              

}		
	 /*override def !(msg: Any): Unit =
		{

		println("Logging starts",logger)
		msg match {
		case msgFinal(msg: String) => {
		log.writeFileInfo(msg)
		super.!(msg)
		}
		super.!(msg)
		}
	    }*/


        
        /* The count of the actors are stored which is used for convergence in Gossip and Push-Sum
           The processing time is also printed here */

        class counting(nrOfActors : Int, baseSystem : ActorSystem) extends Actor
        {
            val count = new Array[Int](nrOfActors)
            var count2 = 0
            var start : Long = _
	    var clock2: Int = 0
            def receive = 
            { 
              case start_time(start_t : Long) =>
               start = start_t
              case counter(count1 : Int, i :Int,nodes,log1)=>
		    
                    count(i) = count1
                    if(count(i) == 1)   // checking this so that for each actor count increases only once.
                      count2 = count2 + 1
              if(count2 == nrOfActors)
              { 
                println("Time Taken: ")
                println(((System.currentTimeMillis - start)/1000.0) + "Seconds")
	//	clock2 = clock1 + 1
	
		for (i <- 0 until nodes.length)
		  {
		    //println("closing files")
		   if (!nodes(i).isTerminated)          
       		  nodes(i) ! PoisonPill
         

		  }
		  System.exit(0)
              } 

        
                
            }

        }
    
        //Here the messages are sent to worker actors for Gossip and Push-Sum
        class Master(nrOfActors : Int) extends Actor  
        {

          var i:Int = 0
          def receive = 
            {
            case Gossip(arActors : List[ActorRef]) =>
                var startNode = Random.nextInt(nrOfActors)
                arActors(startNode) ! gossipWork(startNode, -1)


            }
              
        }

    // Create an Akka system
        def main(args: Array[String]) = 
        {
	    	
                  
                  var nNodes:Int = 20
                  var topology:String = "full"
                  var algorithm:String = "gossip"
                  val system = ActorSystem("simulator")
                  var arActors:List[ActorRef] = Nil
                  var i : Int = 0
                  var nNodes_2D : Double = 0.0
                  var neighbour:List[Int] = Nil
                  if ((topology.equalsIgnoreCase("2D")) || (topology.equalsIgnoreCase("Imp2D")))
                  {   // For 2D and Imp2D the value of no of nodes has to be a perfect square so calculating nearest perfect square
                      nNodes_2D = math.sqrt(nNodes)
                      while (nNodes_2D != nNodes_2D.toInt) 
                                                                  
                      {
                          nNodes= nNodes + 1
                          nNodes_2D = math.sqrt(nNodes)
                      }
                  }

                  val master = system.actorOf(Props(new Master(nNodes)))
                  val counter = system.actorOf(Props(new counting(nNodes, system))) 
                  while(i < nNodes)
                  {
                    arActors ::= system.actorOf(Props(new Worker(topology,i))) 
                    i+=1
                  }
                  i = 0
                   while(i < nNodes)
                   {                         
						
			// Initializing each actor with actor list , counter and actor number
			arActors(i) ! Initialize(arActors,counter,(i+1).toDouble)
                      i+=1

                   }
                  
                  
                val start = System.currentTimeMillis;
                counter ! start_time(start)
                if (algorithm.equalsIgnoreCase("gossip"))
                    master ! Gossip(arActors)
                
             
	     }
        }
