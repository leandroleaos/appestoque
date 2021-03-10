import cherrypy
import os
import sys

thisdir = os.path.dirname(os.path.abspath(__file__))
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(thisdir + '/static'))


class estoque_view:

    @cherrypy.expose
    def index(self):
        dtmpl = dict()
        dtmpl['lsos'] = {'lst_dig': 'teste'}
        dtmpl['message'] = 'teste'
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('base.html')
        return tmpl.render(dtmpl)

    def render(self, template=None, ddados=None):
        dtmpl = dict()
        dtmpl['message'] = ''
        dtmpl['session'] = cherrypy.session
        if ddados:
            dtmpl.update(ddados)
        tmpl = env.get_template(template)
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def indextest(self):
        return self.render('index_teste.html')




if __name__ == '__main__':
    thisdir = os.path.dirname(os.path.abspath(__file__))
    os.sys.path.insert(0, thisdir)
    # Add app dir to path for testing cpcgiserver
    APPDDIR = os.path.abspath(os.path.join(thisdir, os.path.pardir, os.path.pardir))
    sys.path.insert(0, APPDDIR)

    # locale.setlocale(locale.LC_ALL,'pt_BR.utf8')
    # locale.setlocale(locale.LC_ALL,'')
    cherrypy.config.update({
        'tools.staticdir.root': thisdir,
    })
    app = estoque_view()
    app.favicon_ico = cherrypy.tools.staticfile.handler(thisdir+ r'\images\favicon.ico')
    cherrypy.tree.mount(app, config=thisdir + r'\setup.conf')
    cherrypy.config.update(thisdir + r'\setup.conf')


    cherrypy.engine.signals.subscribe()
    cherrypy.engine.start()
    cherrypy.engine.block()
