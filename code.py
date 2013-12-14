import web
import os

render = web.template.render('template/')

urls = (
	'/', 'index',
	'/debug', 'debug'
)

class index:
	def GET(self):
		
		return render.sample()
class debug:
	def GET(self):
		os.system("scalac actorlog.scala >> aa.log")
		os.system("scala example.scala >> aa.log")
		os.system("java parse >> aa.log")
		os.system("dot -Tpng parseActorNo2.dot -o static/dot1.png")
		os.system("mscgen -Tpng -i parseActorNo1.msc -o static/msc1.png")
		return render.sample1()

		
if __name__ == "__main__":

	app = web.application(urls,globals())
	app.run()
