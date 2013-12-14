
import java.io._

object master_clock_obj {
	
		var master_clock : Int = 0
}

trait Log1 {				

	var bf: BufferedWriter = _

def updateClock(clock : Int) = 
{
	if(clock > master_clock_obj.master_clock)
	  	master_clock_obj.master_clock = clock
	else
	  	master_clock_obj.master_clock = master_clock_obj.master_clock + 1
	bf.write("Clock" + master_clock_obj.master_clock + "\n")
}

def writeFileInfo(msg: String, clock : Int) =
	{
	  bf.write("[INFO]" + msg)
	  updateClock(clock)
	
	} 
def writeFileWarn(msg: String, clock : Int) =
	{
	  bf.write("[WARNING]" + msg)
	  updateClock(clock)
	} 
def writeFileError(msg: String, clock : Int) =
	{
	  bf.write("[ERROR]" + msg)
	  updateClock(clock)
	} 
def closeFile(clock : Int)=
	{
		//bf.write("[INFO]" + " Logging stopped")
		//updateClock(clock)	  
		bf.close()
	}
}
class Log(var i: Int) extends Log1
{
   bf = new BufferedWriter(new FileWriter("Actor No" + i + ".txt",true))
}
