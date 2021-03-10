import cherrypy
import os
import sys
import sqlite3
import decimal

thisdir = os.path.dirname(os.path.abspath(__file__))
from jinja2 import Environment, FileSystemLoader


def format_currency_locale(pvalue):
    strpvalue = '0.0'
    try:
        if isinstance( pvalue ,(int,float,decimal.Decimal)):
            value = pvalue
            strpvalue = "{:,.2f}".format(value).replace(",", "X").replace(".", ",").replace("X", ".")
            strpvalue = strpvalue.replace('.',',')
        else:
            if pvalue is None:
                strpvalue = '0,0'
            else:
                strpvalue = pvalue
    except:
        strpvalue = '0.0'
    return strpvalue


env = Environment(loader=FileSystemLoader(thisdir + '/static'))
env.filters['currency'] = format_currency_locale



class estoque_view:
    def __init__(self):
        self.tblstruct = {
            'PRODUTO': 'CODFORNECEDOR, DESCRICAO, PESO, LARGURA, ALTURA, COMPRIMENTO, CLASSFISC, ORIGEM, SITUACAO, GTINEAN, MARCA',
            'ENTRADA': 'IDPROD, DTCAD, DTENTR, FORNECEDOR, QTD, VLRUN, VLRTOT, QTDSALDO',
            'SAIDA': 'IDPROD, DTCAD, DTSAID, MARKETPLACE, QTD, VLRVENDUN, VLRVENDTOT, VLRCUSTOMEDIO, VLRTAXAS, VLRLUCRO, VLRTOTSAID',
        }


    def dbdict(self, sql):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        lstret = list()
        if not cursor.description:
            return []
        columns = [column[0] for column in cursor.description]
        descrel = cursor.description
        for linha in cursor.fetchall():
            row_dict = dict()
            for col in range(len(descrel)):
                row_dict[descrel[col][0]] = linha[col]
            lstret.append(row_dict)
        conn.close()
        return lstret

    def dbinsproddict(self, ddados):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        sql ='''
        insert into PRODUTO (CODFORNECEDOR, DESCRICAO, PESO, LARGURA, ALTURA, COMPRIMENTO, CLASSFISC, ORIGEM, GTINEAN, MARCA, QTDSALDO)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        '''
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        return sql

    def dbinsentrdict(self, ddados):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        print(ddados)
        sql ='''
        insert into ENTRADA (IDPROD,DTCAD, DTENTR, FORNECEDOR, QTD, VLRUN, VLRTOT,QTDSALDO)
        values (?,CURRENT_DATE , ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        self.atualizasaldo(ddados[0])
        return sql

    def dbinssaidadict(self, ddados):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        print(ddados)
        sql ='''
        insert into SAIDA (IDPROD, DTCAD, DTSAID, MARKETPLACE, QTD, VLRVENDUN, VLRVENDTOT, VLRTAXAS, VLRTOTSAID)
        values (?,CURRENT_DATE , ?, ?, ?, ?, ?, ?, ? )
        '''
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        self.atualizasaldo(ddados[0])
        return sql


    def dbupdproddict(self, ddados, vidprod):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        sql ='''
        UPDATE  PRODUTO set CODFORNECEDOR=?,  DESCRICAO=?,  PESO=?,  LARGURA=?,  ALTURA=?
        ,  COMPRIMENTO=?,  CLASSFISC=?,  ORIGEM=?,  SITUACAO=?,  GTINEAN=?,  MARCA=?
        where  IDPROD = ''' + str(vidprod)
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        return sql

    def dbupdentrdict(self, ddados, videntr):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        sql ='''
        UPDATE  ENTRADA set IDPROD=?,  DTENTR=?,  FORNECEDOR=?,  QTD=?
        ,  VLRUN=?,  VLRTOT=?
        where  IDENTR = ''' + str(videntr)
        print('dados')
        print(ddados)
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        self.atualizasaldo(ddados[0])
        return sql

    def dbupdsaidadict(self, ddados, vidsaida):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        sql ='''
        UPDATE  SAIDA set IDPROD=?,  DTSAID=?,  MARKETPLACE=?,  QTD=?
        ,  VLRVENDUN=?,  VLRVENDTOT=?, VLRTAXAS=?,  VLRTOTSAID=?
        where  IDSAID = ''' + str(vidsaida)
        print('dados')
        print(ddados)
        cursor.execute(sql, ddados)
        conn.commit()
        conn.close()
        self.atualizasaldo(ddados[0])
        return sql


    def dbselect(self, sql):
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        lstret = list()
        for linha in cursor.fetchall():
            row = (linha[0], linha[1])
            lstret.append(row)
        conn.close()
        return lstret

    def atualizasaldo(self, codprod):
        dprod = self.dbdict('select * from produto where IDPROD = '+codprod)
        totentr = self.dbdict('SELECT SUM(ENTRADA.QTD) AS QTDENTR FROM ENTRADA WHERE ENTRADA.IDPROD = ' + codprod)
        totsaid = self.dbdict('SELECT SUM(SAIDA.QTD)   AS QTDSAID FROM SAIDA WHERE SAIDA.IDPROD = ' + codprod)
        totger = 0
        if totentr:
            print('totentr',totentr)
            try:
                totger += totentr[0]['QTDENTR']
            except:
                pass
        if totsaid:
            print('totsaid', totsaid)
            try:
                totger -= totsaid[0]['QTDSAID']
            except:
                pass
        conn = sqlite3.connect('db/estoque.db')
        cursor = conn.cursor()
        sql ='UPDATE  PRODUTO set QTDSALDO=? where  IDPROD = ' + str(codprod)
        cursor.execute(sql, (totger,))
        conn.commit()
        conn.close()
        return totger


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

    @cherrypy.expose
    def cadprod(self, **kwargs):
        dados = dict()
        fid = kwargs.get('id', '0')
        print(kwargs)
        retsql = None

        if 'IDPROD' in kwargs.keys():
            vidprod = kwargs.get('IDPROD', '0')
            if vidprod == '':
                vidprod = '0'
            try:
                vidprod = int(vidprod)
            except:
                vidprod = 0
            nmcampos = tuple(self.tblstruct['PRODUTO'].split(','))
            campos = list()

            for n in nmcampos:
                campos.append(kwargs.get(n.strip(), None))
                print('|'+n.strip()+'|')
            print('campos', campos)
            if vidprod > 0:
                # Atualiza
                retsql = self.dbupdproddict(tuple(campos), vidprod)
            else:
                # Insere
                retsql =self.dbinsproddict(campos)
            raise cherrypy.HTTPRedirect("lstprod")

        dados['listadados'] = self.dbdict('select * from produto where IDPROD = '+fid)
        dados['sql'] = retsql
        if dados['listadados']:
            dados['form'] = dados['listadados'][0]
        else:
            dados['form'] = dict()
        #print(dados['listadados'])
        return self.render('cadprod.html', dados)


    @cherrypy.expose
    def cadentrada(self, **kwargs):
        dados = dict()
        fid = kwargs.get('id', '0')
        print(kwargs)
        retsql = None

        if 'IDENTR' in kwargs.keys():
            vidprod = kwargs.get('IDENTR', '0')
            if vidprod == '':
                vidprod = '0'
            try:
                vidprod = int(vidprod)
            except:
                vidprod = 0
            nmcampos = tuple(self.tblstruct['ENTRADA'].split(','))
            campos = list()

            for n in nmcampos:
                nmcampo = n.strip()
                if nmcampo in kwargs.keys():
                    vlrcampo = kwargs.get(nmcampo, '')
                    if 'VLR' in nmcampo:
                        if vlrcampo:
                            vlrcampo = vlrcampo.replace(',', '.')
                        else:
                            vlrcampo = '0'
                    print('nova key', nmcampo, vlrcampo)
                    campos.append(vlrcampo)

            if vidprod > 0:
                # Atualiza
                retsql = self.dbupdentrdict(tuple(campos), vidprod)
            else:
                # Insere
                qtdins = 0
                if 'QTD' in kwargs.keys():
                    campos.append(kwargs.get('QTD', '0'))
                retsql =self.dbinsentrdict(campos)
            raise cherrypy.HTTPRedirect("lstentrada")

        dados['listadados'] = self.dbdict('select * from ENTRADA where IDENTR = '+fid)
        dados['lstprod'] = self.dbselect('select IDPROD,DESCRICAO from produto  order by DESCRICAO')
        dados['sql'] = retsql
        if dados['listadados']:
            dados['form'] = dados['listadados'][0]
            for cmp in dados['form']:
                print('cmp', cmp, dados['form'][cmp])
                if 'VLR' in cmp:
                    dados['form'][cmp] = str(dados['form'][cmp]).replace('.', ',')
                    print('cmp', dados['form'][cmp])


        else:
            dados['form'] = dict()
        #print(dados['listadados'])
        return self.render('cadentrada.html', dados)

    @cherrypy.expose
    def cadsaida(self, **kwargs):
        dados = dict()
        fid = kwargs.get('id', '0')
        print(kwargs)
        retsql = None

        if 'IDSAID' in kwargs.keys():
            vidprod = kwargs.get('IDSAID', '0')
            if vidprod == '':
                vidprod = '0'
            try:
                vidprod = int(vidprod)
            except:
                vidprod = 0
            nmcampos = tuple(self.tblstruct['SAIDA'].split(','))
            campos = list()

            for n in nmcampos:
                if n.strip() in kwargs.keys():
                    campos.append(kwargs.get(n.strip(), None))

            if vidprod > 0:
                # Atualiza
                retsql = self.dbupdsaidadict(tuple(campos), vidprod)
            else:
                # Insere
                retsql =self.dbinssaidadict(campos)
            raise cherrypy.HTTPRedirect("lstsaida")

        dados['listadados'] = self.dbdict('select * from SAIDA where IDSAID = '+fid)
        dados['lstprod'] = self.dbselect('select IDPROD,DESCRICAO from produto  order by DESCRICAO')
        dados['sql'] = retsql
        if dados['listadados']:
            dados['form'] = dados['listadados'][0]
        else:
            dados['form'] = dict()
        #print(dados['listadados'])
        return self.render('cadsaida.html', dados)


    @cherrypy.expose
    def lstprod(self, **kwargs):
        dados = dict()
        dados['listadados'] = self.dbdict('select * from produto  order by DESCRICAO')
        return self.render('lstprod.html', dados)

    @cherrypy.expose
    def lstentrada(self, **kwargs):
        dados = dict()
        sql = '''
        select ENTRADA.*, PRODUTO.DESCRICAO 
        from ENTRADA
          LEFT JOIN PRODUTO ON PRODUTO.IDPROD = ENTRADA.IDPROD 
        order by DTENTR
        '''
        dados['listadados'] = self.dbdict(sql)
        return self.render('lstentrada.html', dados)

    @cherrypy.expose
    def lstsaida(self, **kwargs):
        dados = dict()
        sql = '''
        select SAIDA.*, PRODUTO.DESCRICAO 
        from SAIDA
          LEFT JOIN PRODUTO ON PRODUTO.IDPROD = SAIDA.IDPROD 
        order by DTSAID
        '''
        dados['listadados'] = self.dbdict(sql)
        return self.render('lstsaida.html', dados)



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
