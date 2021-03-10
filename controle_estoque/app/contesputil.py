#!/usr/bin/python
#-*- coding: utf-8 -*-

#Banco de dados utilizados
import fdb
#kinterbasdb.init(type_conv=300)

#Trabalhando com cgi
import cgi 
import cgitb
#Rotinas para manipulacao de data
import datetime
import time
import locale
import hashlib, Cookie, os
import math
import decimal
import sys
import re

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText




def add_row(tpcursor, tablename, rowdict):
    '''
    Insere um registro na tabela indicada (monta sql)
    :param cursor:
    :param tablename:
    :param rowdict:
    :return:
    '''
    # XXX tablename not sanitized
    # XXX test for allowed keys is case-sensitive

    # filter out keys that are not column names
    cursor = ret_cursor(tpcursor)
    cursor.execute('''SELECT RDB$FIELD_NAME  FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME= '%s' ''' % tablename)
    allowed_keys = set(row[0].strip() for row in cursor.fetchall())
    keys = allowed_keys.intersection(rowdict.keys())
    print "allowed_keys"
    print allowed_keys
    print 'keys', keys

    if len(rowdict) > len(keys):
        unknown_keys = set(rowdict.keys()) - allowed_keys
        print >> sys.stderr, "skipping keys:", ", ".join(unknown_keys)

    columns = ", ".join(keys)
    values_template = ", ".join(["?"] * len(keys))

    sql = "insert into %s (%s) values (%s)" % (tablename, columns, values_template)
    values = tuple(rowdict[key] for key in keys)
    print 'insert'
    print sql
    print values
    execsqlp(sql, values, tpcursor)
    return 1

def update_row(tpcursor, nmtabela, primary, campos):
    # filter out keys that are not column names
    cursor = ret_cursor(tpcursor)
    cursor.execute('''SELECT RDB$FIELD_NAME  FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME= '%s' ''' % nmtabela)
    allowed_keys = set(row[0].strip() for row in cursor.fetchall())
    lstcampos = list(allowed_keys)
    print 'lstcampos', lstcampos
    print 'allowed keys', allowed_keys

    keys = allowed_keys.intersection(campos.keys())

    if len(campos) > len(keys):
        unknown_keys = set(campos.keys()) - allowed_keys
        print "skipping keys:", ", ".join(unknown_keys)

    sql = "update %s set " % nmtabela
    for key in keys:
        if key != primary:
            sql += ''' %s = %s ,''' % (str(key), formatfb(campos[key]))

    if sql[-1] == ',':
        sql = sql[:-1]
    sql += ''' where  %s = %s  ''' % (primary, formatfb(campos[primary]))
    print 'update'
    print sql
    execsql(sql, 'fb')
    return 1


def formathtml(strvar):
    result = u''
    #print strvar
    #print type(strvar)
    if type(strvar) == str:
        result = unicode(strvar.decode('latin-1'))
    elif type(strvar) == unicode:
        result = strvar
    elif type(strvar) == datetime.datetime:
        result = unicode(strvar.strftime('%d/%m/%Y'))
    elif type(strvar) == datetime.date:
        result = unicode(strvar.strftime('%d/%m/%Y'))
    elif isinstance(strvar, (float, decimal.Decimal)):
        result = "{:,.2f}".format(strvar).replace(",", "X").replace(".", ",").replace("X", ".")
    elif isinstance(strvar, (int, long)):
        result = unicode(strvar)
    elif strvar is None:
        result = u''
    else:
        result = strvar
    return result

def formatfb(strvar):
    result = None
    #print strvar
    #print type(strvar)
    if type(strvar) == str:
        result=strvar.encode('latin-1')
    elif type(strvar) == unicode:
        result=strvar
    elif type(strvar) == datetime.datetime:
        result=unicode(strvar.strftime('%d.%m.%Y'))
    elif type(strvar) == datetime.date:
        result=unicode(strvar.strftime('%d.%m.%Y'))
    elif isinstance( strvar ,(float,decimal.Decimal)):
        result = "{:.2f}".format(strvar)
    elif isinstance( strvar ,(int,long)):
        result = unicode(strvar)
    elif strvar is None:
        result = 'Null'
    else:
        result = strvar
    if result and (result <> 'Null') and (result <> ''):
        result = "'"+result+"'"
    else:
        result = 'Null'
    return result



def is_logado():
    cookie = Cookie.SimpleCookie()
    string_cookie = os.environ.get('HTTP_COOKIE')

    if not string_cookie :
        sid = ''
        susernm = ''
    else:
        cookie.load(string_cookie)
        if cookie.has_key("sid") :
            sid = cookie['sid'].value
        else:
            sid = ''

        if cookie.has_key("susernm") :
            susernm = cookie["susernm"].value
        else:
            cookie["susernm"]=''
            susernm=''


    #print "<br>sid="+sid
    #print "<br>susernm="+susernm

    if ( sid != '' ) :
        conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTESP.GDB',
        user='sysdba',
        password='masterkey'
        )
        cursor = conn.cursor()
        cursor.execute("select 1 from websession where  SESSION_STR = '%s' " %  sid )
        rowr = cursor.fetchone()
        if rowr:
            return True
        else :
            return False
    else :
        return False


def avisa_protocolo_online(idprot,nmuser=None):
    paramsql = {'idprot': idprot, 'nmuser': nmuser}
    Fssqlprot='''select protocolo2.* , cliente.EMAIL ,cliente.EMAILPESSOAL,cliente.EMAILFISCAL,cliente.EMAILCONTABIL,cliente.EMAILFINANCEIRO,depto.NM_SETOR,cliente.CLI_CODDOMINIO from protocolo2
     left join cliente on cliente.codigo=protocolo2.CODCLIREP
     left join depto on depto.DESCRICAO= protocolo2.DEPTOUSER
     where protocolo2.idprot2= %(idprot)s
      ''' % paramsql
    Fssqlserv='''select servicos2.* , tpserv.clirep as servcro ,tpserv.competencia as boolcomp, tpserv.venc as boolvenc,tpserv.valor as boolval
     ,tpserv.email as boolemail , tpserv.nome as servnome ,tpserv.descrprot as nomprot
     ,tpserv.prot as boolprot , tpserv.TITULOAVULS from SERVICOS2
     left outer join tpserv on (tpserv.idtpserv=servicos2.idservico)
     where IDPROT2= %(idprot)s
     order by SERVICOS2.IDPROTSERV
      ''' % paramsql

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqlprot.encode('ascii'))
    lstprotocolo = dictcursor2(cursorprot)
    if lstprotocolo:
        dprot = lstprotocolo[0]
        print(dprot)
    else:
        raise NameError('Protocolo invalido')

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqlserv.encode('ascii'))
    dprotserv = dictcursor2(cursorprot)

    if not nmuser:
        paramsql['nmuser'] = dprot['USUARIO']

    Fssqluser='''select * from usuario where nome = '%(nmuser)s'
      ''' % paramsql

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqluser.encode('ascii'))
    lstprotuser = dictcursor2(cursorprot)
    if lstprotuser:
        duser = lstprotuser[0]
    else:
        raise NameError('Usuario invalido')


    strlstserv = u'<ul>'
    for s in dprotserv:
        strlstserv += u'<li>'+s['SERVNOME']
        if s['DESCRAVULSA']:
            strlstserv += u' - '+s['TITULOAVULS']+' '+s['DESCRAVULSA']
        strlstserv += u'</li>'
    strlstserv += u'</ul>'
    dprot['LSTSERV'] = strlstserv
    print strlstserv

    emailcliente = ''
    emailcco = ''
    if dprot['EMAILPADRAO'] == u'S':
        if dprot['DEPTOUSER'] == u'DEP. PESSOAL':
            emailcliente = str(dprot['EMAILPESSOAL'])
        elif dprot['DEPTOUSER'] == u'DEP. FISCAL':
            emailcliente = str(dprot['EMAILFISCAL'])
        elif dprot['DEPTOUSER'] == u'DEP. CONTÁBIL':
            emailcliente = str(dprot['EMAILCONTABIL'])
        elif dprot['DEPTOUSER'] == u'FINANCEIRO':
            emailcliente = str(dprot['EMAILFINANCEIRO'])
        elif dprot['DEPTOUSER'] == u'ADMINISTRAÇÃO':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'INFORMÁTICA':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'EXPEDIÇÃO':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'LEGALIZAÇÃO':
            emailcliente = ''
        else:
            emailcliente = dprot['EMAIL']
    if '*' in emailcliente:
        emailcco = emailcliente.replace('*', '')
        emailcliente = ''

    if dprot['EMAIL_RESP2'] == u'' and emailcliente == '':
        raise NameError('email destino invalido')

    txtmsg = u'''
         <html><head><meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
         <style rel="stylesheet" type="text/css">
          body { font-family: Tahoma, Verdana, Segoe, sans-serif; font-size: 12px; max-width:700px; }
          .titulo { background: Black;color:white; text-align: center; font-family: Arial; font-size: 22px;}
          a { color:red; font-family: Arial; font-size: 15px;}
          .rodape { background: Black;color:white; font-family: Arial; font-size: 10px;padding:10px; }
          </style>
            </head><body>
         <br><br>
         <div class="titulo" >Contesp® Contabilidade </div>
         <br>
         Cliente: <strong>%(NOME)s</strong>
         <br><br>
         Prezado Sr(a). %(RESPONSAVEL2)s;
         <br>
         Informamos que há novos documentos da área %(NM_SETOR)s  aos seus cuidados disponíveis para visualização e que devem ser analisados imediatamente. <br>
         <a href="http://contesp.com.br/rpa/avisprote?q=%(IDPROT2)s&s=%(MD5)s">Clique aqui</a>
          para ser redirecionado ao site da Contesp® Contabilidade e visualizar os documentos do <strong>protocolo id = %(IDPROT2)s</strong>, conforme itens relacionados abaixo:
         <br><br>
          %(LSTSERV)s
         <br><br>
         Muito obrigado!<br>
          %(USUARIO)s
         <br>
         <br><br><div class="rodape" >
          Contesp® Contabilidade
          <br>Av. Monteiro Lobato, 6006 e 6020  -  Jd. Cumbica - Guarulhos - SP - Cep: 07180-000
          <br>Telefone: (11) 2413-3333   /  Site: www.contesp.com.br   /   e-mail: contesp@contesp.com.br
          <br> </div></body>
            ''' % dprot

    SUBJECT = u"Aviso de Protocolo :"+unicode(dprot['IDPROT2'])+': '+dprot['ASSUNTO']
    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.

    msg = MIMEMultipart('alternative')
    msg['Subject'] = SUBJECT
    msg['From'] = duser['EMAIL']
    msg['To'] = dprot['EMAIL_RESP2']+';'+emailcliente
    msg['CC'] = 'controle@contesp.com.br;'+duser['EMAIL']+';'+duser['EMAILCC']
    msg['BCC'] = emailcco
    msg.add_header('Reply-To', duser['EMAIL'])
    TO = msg['To'].split(';') + msg['CC'].split(';') + msg['BCC'].split(';')
    TO = [x.strip() for x in TO if x.strip()]
    print TO
    print msg['CC'].split(';')
    part1 = MIMEText(txtmsg.encode('utf-8'),_subtype='html', _charset='utf-8')
    msg.attach(part1)
    server = smtplib.SMTP()
    server.set_debuglevel(1)
    server.connect('192.168.0.2')
    server.ehlo()
    server.esmtp_features['auth'] = 'LOGIN DIGEST-MD5 '
    server.login(duser['IDEMAIL'], duser['SENHAEMAIL'])
    server.sendmail(duser['EMAIL'], TO, msg.as_string())
    server.quit()



def avisa_protocolo_onlinew(idprot,nmuser=None):
    paramsql = {'idprot': idprot, 'nmuser': nmuser}
    Fssqlprot='''select protocolo2.* , cliente.EMAIL ,cliente.EMAILPESSOAL,cliente.EMAILFISCAL,cliente.EMAILCONTABIL,cliente.EMAILFINANCEIRO,depto.NM_SETOR,cliente.CLI_CODDOMINIO from protocolo2
     left join cliente on cliente.codigo=protocolo2.CODCLIREP
     left join depto on depto.DESCRICAO= protocolo2.DEPTOUSER
     where protocolo2.idprot2= %(idprot)s
      ''' % paramsql
    Fssqlserv='''select servicos2.* , tpserv.clirep as servcro ,tpserv.competencia as boolcomp, tpserv.venc as boolvenc,tpserv.valor as boolval
     ,tpserv.email as boolemail , tpserv.nome as servnome ,tpserv.descrprot as nomprot
     ,tpserv.prot as boolprot , tpserv.TITULOAVULS from SERVICOS2
     left outer join tpserv on (tpserv.idtpserv=servicos2.idservico)
     where IDPROT2= %(idprot)s
     order by SERVICOS2.IDPROTSERV
      ''' % paramsql

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqlprot.encode('ascii'))
    lstprotocolo = dictcursor2(cursorprot)
    if lstprotocolo:
        dprot = lstprotocolo[0]
        print(dprot)
    else:
        raise NameError('Protocolo invalido')

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqlserv.encode('ascii'))
    dprotserv = dictcursor2(cursorprot)

    if not nmuser:
        paramsql['nmuser'] = dprot['USUARIO']

    Fssqluser='''select * from usuario where nome = '%(nmuser)s'
      ''' % paramsql

    cursorprot = ret_cursor('fb')
    cursorprot.execute(Fssqluser.encode('ascii'))
    lstprotuser = dictcursor2(cursorprot)
    if lstprotuser:
        duser = lstprotuser[0]
    else:
        raise NameError('Usuario invalido')


    strlstserv = u'<ul>'
    for s in dprotserv:
        strlstserv += u'<li>'+s['SERVNOME']
        if s['DESCRAVULSA']:
            strlstserv += u' - '+s['TITULOAVULS']+' '+s['DESCRAVULSA']
        strlstserv += u'</li>'
    strlstserv += u'</ul>'
    dprot['LSTSERV'] = strlstserv
    print strlstserv

    emailcliente = ''
    emailcco = ''
    if dprot['EMAILPADRAO'] == u'S':
        if dprot['DEPTOUSER'] == u'DEP. PESSOAL':
            emailcliente = str(dprot['EMAILPESSOAL'])
        elif dprot['DEPTOUSER'] == u'DEP. FISCAL':
            emailcliente = str(dprot['EMAILFISCAL'])
        elif dprot['DEPTOUSER'] == u'DEP. CONTÁBIL':
            emailcliente = str(dprot['EMAILCONTABIL'])
        elif dprot['DEPTOUSER'] == u'FINANCEIRO':
            emailcliente = str(dprot['EMAILFINANCEIRO'])
        elif dprot['DEPTOUSER'] == u'ADMINISTRAÇÃO':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'INFORMÁTICA':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'EXPEDIÇÃO':
            emailcliente = ''
        elif dprot['DEPTOUSER'] == u'LEGALIZAÇÃO':
            emailcliente = ''
        else:
            emailcliente = dprot['EMAIL']
    if '*' in emailcliente:
        emailcco = emailcliente.replace('*', '')
        emailcliente = ''

    if dprot['EMAIL_RESP2'] == u'' and emailcliente == '':
        raise NameError('email destino invalido')

    txtmsg = u'''
         <html><head><meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
         <style rel="stylesheet" type="text/css">
          body { font-family: Tahoma, Verdana, Segoe, sans-serif; font-size: 12px; max-width:700px; }
          .titulo { background: Black;color:white; text-align: center; font-family: Arial; font-size: 22px;}
          a { color:red; font-family: Arial; font-size: 15px;}
          .rodape { background: Black;color:white; font-family: Arial; font-size: 10px;padding:10px; }
          </style>
            </head><body>
         <br><br>
         <div class="titulo" >Contesp® Contabilidade </div>
         <br>
         Cliente: <strong>%(NOME)s</strong>
         <br><br>
         Prezado Sr(a). %(RESPONSAVEL2)s;
         <br>
         Informamos que há novos documentos da área %(NM_SETOR)s  aos seus cuidados disponíveis para visualização e que devem ser analisados imediatamente. <br>
         <a href="http://contesp.com.br/rpa/avisprote?q=%(IDPROT2)s&s=%(MD5)s">Clique aqui</a>
          para ser redirecionado ao site da Contesp® Contabilidade e visualizar os documentos do <strong>protocolo id = %(IDPROT2)s</strong>, conforme itens relacionados abaixo:
         <br><br>
          %(LSTSERV)s
         <br><br>
         Muito obrigado!<br>
          %(USUARIO)s
         <br>
         <br><br><div class="rodape" >
          Contesp® Contabilidade
          <br>Av. Monteiro Lobato, 6006 e 6020  -  Jd. Cumbica - Guarulhos - SP - Cep: 07180-000
          <br>Telefone: (11) 2413-3333   /  Site: www.contesp.com.br   /   e-mail: contesp@contesp.com.br
          <br> </div></body>
            ''' % dprot

    SUBJECT = u"Aviso de Protocolo :"+unicode(dprot['IDPROT2'])+': '+dprot['ASSUNTO']
    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.

    msg = MIMEMultipart('alternative')
    msg['Subject'] = SUBJECT
    msg['From'] = duser['EMAIL']
    msg['To'] = dprot['EMAIL_RESP2']+';'+emailcliente
    msg['CC'] = 'controle@contesp.com.br;'+duser['EMAIL']+';'+duser['EMAILCC']
    msg['BCC'] = emailcco
    msg.add_header('Reply-To', duser['EMAIL'])
    TO = msg['To'].split(';') + msg['CC'].split(';') + msg['BCC'].split(';')
    TO = [x.strip() for x in TO if x.strip()]
    print TO
    print msg['CC'].split(';')
    part1 = MIMEText(txtmsg.encode('utf-8'),_subtype='html', _charset='utf-8')
    msg.attach(part1)
    server = smtplib.SMTP()
    server.set_debuglevel(1)
    server.connect('192.168.0.2')
    server.ehlo()
    server.esmtp_features['auth'] = 'LOGIN DIGEST-MD5 '
    server.login(duser['IDEMAIL'], duser['SENHAEMAIL'])
    server.sendmail(duser['EMAIL'], TO, msg.as_string())
    server.quit()


def sqlstr(vlr):
    texto=''
    if (type(vlr) ==str) : texto=normalize('NFKD', vlr.decode('latin-1')).encode('ASCII', 'ignore')
    return texto

def execsqlp(strsql,param,tipo):
    if (tipo=='fb'):
        conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTESP.GDB',
        user='sysdba',
        password='masterkey'
        )
        cursor = conn.cursor()

    if (tipo=='do'):
        conn = pyodbc.connect("DSN=serverbd1;UID=root;PWD=server")
        cursor = conn.cursor()

    cursor.execute(strsql, param)
    #print param
    #print strsql
    cursor.close()
    conn.commit()

    return 1


def execsql(strsql,tipo):
    if (tipo=='fb'):
        conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTESP.GDB',
        user='sysdba',
        password='masterkey'
        )
        cursor = conn.cursor()

    if (tipo=='do'):
        import pyodbc
        conn = pyodbc.connect("DSN=serverbd1 ;UID=root;PWD=server")
        cursor = conn.cursor()

    cursor.execute(strsql)
    #print strsql
    cursor.close()
    conn.commit()

    return 1

def ret_cursorufb():
    conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTESP.GDB',
        user='sysdba',
        password='masterkey',
        charset='UTF8'
    )
    cursorfb = conn.cursor()
    return cursorfb


def ret_cursor(tipo):
    if (tipo=='fb'):
        conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTESP.GDB',
        user='sysdba',
        password='masterkey'
        )
        cursor = conn.cursor()

    if (tipo=='fin'):
        conn = fdb.connect(
        host='192.168.0.20',
        database='/home/bd2/Bco_dados/CONTROLECONTAS.GDB',
        user='sysdba',
        password='masterkey'
        )
        cursor = conn.cursor()

    if (tipo=='do'):
        import pyodbc
        conn = pyodbc.connect("DSN=Contabil;UID=root;PWD=server")
        conn.timeout = 60
        cursor = conn.cursor()

    if (tipo=='doproducao'):
        import pyodbc
        conn = pyodbc.connect("DSN=serverbd1;UID=root;PWD=server")
        cursor = conn.cursor()

    if (tipo=='dotemporario'):
        import pyodbc
        conn = pyodbc.connect("DSN=espelho;UID=root;PWD=server")
        cursor = conn.cursor()
    return cursor

class htmltag:
    tipoconteudohtml="Content-type: text/html; latin-1"
    htmlini='''<html>  
                 <head>  
                         <title>%s</title>  
                 <head>  
                 <body>  
                         '''
    htmlfim='''
                 </body>  
         <html>'''
    tableini=''' <table align="center"  border="1" > '''
    tablefim=" </table> "


def geterros(objeto , atrib):
    return objeto.get(atrib ,'')

def to_utf(valor):
    vlrret=u''
    if type(valor) == str:
        try:
            vlrret=unicode(valor.decode('utf-8'))
        except UnicodeDecodeError, UnicodeEncodeError :
            vlrret=unicode(valor.decode('latin-1'))
    elif  type( valor)==datetime.datetime:
        try:
            vlrret=unicode(valor.strftime('%d/%m/%Y'))
        except:
            vlrret=unicode(valor)
    elif  type( valor)==datetime.date:
        try:
            vlrret=unicode( row[col].strftime('%d/%m/%Y') )
        except:
            vlrret=unicode( row[col] )
    elif  valor is None:
        vlrret=unicode('')
    else :
        try:
            vlrret=unicode(valor)
        except UnicodeDecodeError, UnicodeEncodeError :
            vlrret=unicode('')
    return vlrret

def dictcursor2(cursor):
    if not cursor.description :
        return []
    columns = [column[0] for column in cursor.description]
    descrel = cursor.description
    results = []
    for row in cursor.fetchall():
        row_dict = dict()
        for col in range(len(descrel)):
            if type( row[col])==str:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].decode('utf-8'))
                except UnicodeDecodeError, UnicodeEncodeError :
                    row_dict[descrel[col][0]]=unicode(row[col].decode('latin-1'))
            elif  type( row[col])==datetime.datetime:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].strftime('%d/%m/%Y'))
                except:
                    row_dict[descrel[col][0]]=unicode(row[col])
            elif  type( row[col])==datetime.date:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].strftime('%d/%m/%Y'))
                except:
                    row_dict[descrel[col][0]]=unicode(row[col])
            elif  row[col] is None:
                row_dict[descrel[col][0]]=unicode('')
            else :
                row_dict[descrel[col][0]]=row[col]
        results.append(row_dict)
    return results


def dictcursor2jsn(cursor,dtformat='%Y-%m-%d'):
    if not cursor.description :
        return []
    columns = [column[0] for column in cursor.description]
    descrel = cursor.description
    results = []
    for row in cursor.fetchall():
        row_dict = dict()
        for col in range(len(descrel)):
            if type( row[col])==str:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].decode('utf-8'))
                except UnicodeDecodeError, UnicodeEncodeError :
                    row_dict[descrel[col][0]]=unicode(row[col].decode('latin-1'))
            elif  type( row[col])==datetime.datetime:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].strftime(dtformat))
                except:
                    row_dict[descrel[col][0]]=unicode(row[col])
            elif  type( row[col])==datetime.date:
                try:
                    row_dict[descrel[col][0]]=unicode(row[col].strftime(dtformat))
                except:
                    row_dict[descrel[col][0]]=unicode(row[col])
            elif  row[col] is None:
                row_dict[descrel[col][0]]=unicode('')
            else :
                row_dict[descrel[col][0]]=row[col]
        results.append(row_dict)
    return results


def dictcursor2l(cursor):
    columns = [column[0] for column in cursor.description]
    descrel = cursor.description
    results = []
    for row in cursor.fetchall():
        row_dict = dict()
        for col in range(len(descrel)):
            if type( row[col])==str:
                row_dict[descrel[col][0]]=row[col].decode('latin-1' )
            elif  type( row[col])==datetime.datetime:
                row_dict[descrel[col][0]]=str(row[col].strftime('%d/%m/%Y'))
            elif  type( row[col])==datetime.date:
                row_dict[descrel[col][0]]=str(row[col].strftime('%d/%m/%Y'))
            elif  row[col] is None:
                row_dict[descrel[col][0]]=str('')
            else :
                row_dict[descrel[col][0]]=row[col]
        results.append(row_dict)
    return results

def dictcursor3(cursor):
    columns = [column[0] for column in cursor.description]
    descrel = cursor.description
    results = []
    for row in cursor.fetchall():
        row_dict = dict()
        for col in range(len(descrel)):
            if type( row[col])==str:
                row_dict[descrel[col][0]]=unicode(row[col].decode('latin-1' ))
            elif  row[col] is None:
                row_dict[descrel[col][0]]=unicode('')
            else :
                row_dict[descrel[col][0]]=row[col]
        results.append(row_dict)
    return results

def dictcursor(cursor):
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
        #print row
    return results

def diasuteis(fromdate,todate):
    daycount = (todate - fromdate).days
    daygenerator = (fromdate + datetime.timedelta(x + 1) for x in xrange(daycount))
    totdays =sum(1 for day in daygenerator if day.weekday() < 5)
    mes = fromdate.month
    ano = fromdate.year
    if (mes==12) and (ano==2013):
        totdays -= 8
    return totdays

def ultimodia(dt0):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 + datetime.timedelta(days=32)
    dt3 = dt2.replace(day=1)
    dt4 = dt3 - datetime.timedelta(days=1)
    return dt4

def addmonths(d,x):
    newmonth = ((( d.month -1 ) + x ) % 12 ) + 1
    newyear  = d.year + ((( d.month -1 ) + x ) // 12 )
    return datetime.date( newyear , newmonth , d.day )

def subtract_one_month(dt0):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 - datetime.timedelta(days=1)
    dt3 = dt2.replace(day=1)
    return dt3
    
def add_one_month(dt0):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 + datetime.timedelta(days=32)
    dt3 = dt2.replace(day=1)
    return dt3

def mkDateTime(dateString,strFormat="%Y-%m-%d"):
    # Expects "YYYY-MM-DD" string
    # returns a datetime object
    eSeconds = time.mktime(time.strptime(dateString,strFormat))
    return datetime.datetime.fromtimestamp(eSeconds)

def validdate(date_text):
    vdate = False
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        vdate = True
    except ValueError:
        vdate = False
    return vdate

def validdateponto(date_text):
    vdate = False
    try:
        datetime.datetime.strptime(date_text, '%d.%m.%Y')
        vdate = True
    except ValueError:
        vdate = False
    return vdate


def formatDate(dtDateTime,strFormat="%Y-%m-%d"):
    # format a datetime object as YYYY-MM-DD string and return
    strdate=dtDateTime
    if dtDateTime:
        if isinstance(dtDateTime, (datetime.date, datetime.datetime)):
            strdate = dtDateTime.strftime(strFormat)
    if dtDateTime == None :
        strdate = ''
    return strdate

def format_currency(value):
    return "{:.2f}".format(value)

def format_currency_locale(pvalue):
    strpvalue = '0.0'

    try:
        if isinstance( pvalue ,(int,float,long,decimal.Decimal)):
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

def format_currency_locale_tmp(pvalue):
    strpvalue = '0.0'

    try:
        if isinstance( pvalue ,(int,float,long,decimal.Decimal)):
            value = pvalue
            strpvalue = locale.format("%.2f", value ,grouping=True,monetary=True)
        else:
            if pvalue is None:
                strpvalue = locale.format("%.2f", 0 ,grouping=True,monetary=True)
            else:
                strpvalue = pvalue
    except:
        strpvalue = '0.0'
    return strpvalue


def format_currency_locale2(pvalue):
    try:
        value=math.floor( float(pvalue) *100 ) / 100
    except:
        value=0.0
    if value is None:
        value=0.0
    return locale.format("%.2f", value ,grouping=True,monetary=True)

def hora_atual():
    return datetime.datetime.now().strftime("%d/%m/%Y  %H:%M")

def mkFirstOfMonth2(dtDateTime):
    #what is the first day of the current month
    ddays = int(dtDateTime.strftime("%d"))-1 #days to subtract to get to the 1st
    delta = datetime.timedelta(days= ddays)  #create a delta datetime object
    return dtDateTime - delta                #Subtract delta and return

def mkFirstOfMonth(dtDateTime):
    #what is the first day of the current month
    #format the year and month + 01 for the current datetime, then form it back
    #into a datetime object
    return mkDateTime(formatDate(dtDateTime,"%Y-%m-01"))

def mkLastOfMonth(dtDateTime):
    dYear = dtDateTime.strftime("%Y")        #get the year
    dMonth = str(int(dtDateTime.strftime("%m"))%12+1)#get next month, watch rollover
    dDay = "1"                               #first day of next month
    nextMonth = mkDateTime("%s-%s-%s"%(dYear,dMonth,dDay))#make a datetime obj for 1st of next month
    delta = datetime.timedelta(seconds=1)    #create a delta of 1 second
    return nextMonth - delta                 #subtract from nextMonth and return

def table_html(lol):
    tbl = '<table border="1" >'
    if len(lol) > 0:
        if isinstance(lol[0],dict):
            tbl += '<thead style="color:green;">  <tr>'
            for a in lol[0]:
                tbl += '    <th>%s</th>' % str(a)
            tbl += '  </tr> </thead>'
    for sublist in lol:
        if isinstance(sublist, dict):
            tbl += '  <tr>'
            for a in sublist:
                tbl += '    <td>%s</td>' % str(sublist[a])
            tbl += '  </tr>'
        else:
            tbl += '  <tr><td>'
            tbl += '    </td><td>'.join(sublist)
            tbl += '  </td></tr>'
    tbl += '</table>'
    return tbl


def striprtf(text):
   pattern = re.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", re.I)
   # control words which specify a "destionation".
   destinations = frozenset((
      'aftncn','aftnsep','aftnsepc','annotation','atnauthor','atndate','atnicn','atnid',
      'atnparent','atnref','atntime','atrfend','atrfstart','author','background',
      'bkmkend','bkmkstart','blipuid','buptim','category','colorschememapping',
      'colortbl','comment','company','creatim','datafield','datastore','defchp','defpap',
      'do','doccomm','docvar','dptxbxtext','ebcend','ebcstart','factoidname','falt',
      'fchars','ffdeftext','ffentrymcr','ffexitmcr','ffformat','ffhelptext','ffl',
      'ffname','ffstattext','field','file','filetbl','fldinst','fldrslt','fldtype',
      'fname','fontemb','fontfile','fonttbl','footer','footerf','footerl','footerr',
      'footnote','formfield','ftncn','ftnsep','ftnsepc','g','generator','gridtbl',
      'header','headerf','headerl','headerr','hl','hlfr','hlinkbase','hlloc','hlsrc',
      'hsv','htmltag','info','keycode','keywords','latentstyles','lchars','levelnumbers',
      'leveltext','lfolevel','linkval','list','listlevel','listname','listoverride',
      'listoverridetable','listpicture','liststylename','listtable','listtext',
      'lsdlockedexcept','macc','maccPr','mailmerge','maln','malnScr','manager','margPr',
      'mbar','mbarPr','mbaseJc','mbegChr','mborderBox','mborderBoxPr','mbox','mboxPr',
      'mchr','mcount','mctrlPr','md','mdeg','mdegHide','mden','mdiff','mdPr','me',
      'mendChr','meqArr','meqArrPr','mf','mfName','mfPr','mfunc','mfuncPr','mgroupChr',
      'mgroupChrPr','mgrow','mhideBot','mhideLeft','mhideRight','mhideTop','mhtmltag',
      'mlim','mlimloc','mlimlow','mlimlowPr','mlimupp','mlimuppPr','mm','mmaddfieldname',
      'mmath','mmathPict','mmathPr','mmaxdist','mmc','mmcJc','mmconnectstr',
      'mmconnectstrdata','mmcPr','mmcs','mmdatasource','mmheadersource','mmmailsubject',
      'mmodso','mmodsofilter','mmodsofldmpdata','mmodsomappedname','mmodsoname',
      'mmodsorecipdata','mmodsosort','mmodsosrc','mmodsotable','mmodsoudl',
      'mmodsoudldata','mmodsouniquetag','mmPr','mmquery','mmr','mnary','mnaryPr',
      'mnoBreak','mnum','mobjDist','moMath','moMathPara','moMathParaPr','mopEmu',
      'mphant','mphantPr','mplcHide','mpos','mr','mrad','mradPr','mrPr','msepChr',
      'mshow','mshp','msPre','msPrePr','msSub','msSubPr','msSubSup','msSubSupPr','msSup',
      'msSupPr','mstrikeBLTR','mstrikeH','mstrikeTLBR','mstrikeV','msub','msubHide',
      'msup','msupHide','mtransp','mtype','mvertJc','mvfmf','mvfml','mvtof','mvtol',
      'mzeroAsc','mzeroDesc','mzeroWid','nesttableprops','nextfile','nonesttables',
      'objalias','objclass','objdata','object','objname','objsect','objtime','oldcprops',
      'oldpprops','oldsprops','oldtprops','oleclsid','operator','panose','password',
      'passwordhash','pgp','pgptbl','picprop','pict','pn','pnseclvl','pntext','pntxta',
      'pntxtb','printim','private','propname','protend','protstart','protusertbl','pxe',
      'result','revtbl','revtim','rsidtbl','rxe','shp','shpgrp','shpinst',
      'shppict','shprslt','shptxt','sn','sp','staticval','stylesheet','subject','sv',
      'svb','tc','template','themedata','title','txe','ud','upr','userprops',
      'wgrffmtfilter','windowcaption','writereservation','writereservhash','xe','xform',
      'xmlattrname','xmlattrvalue','xmlclose','xmlname','xmlnstbl',
      'xmlopen',
   ))
   # Translation of some special characters.
   specialchars = {
      'par': '\n',
      'sect': '\n\n',
      'page': '\n\n',
      'line': '\n',
      'tab': '\t',
      'cell': '\t| ',
      'row': '\n',
      'emdash': '\u2014',
      'endash': '\u2013',
      'emspace': '\u2003',
      'enspace': '\u2002',
      'qmspace': '\u2005',
      'bullet': '\u2022',
      'lquote': '\u2018',
      'rquote': '\u2019',
      'ldblquote': '\201C',
      'rdblquote': '\u201D',
   }
   stack = []
   ignorable = False       # Whether this group (and all inside it) are "ignorable".
   ucskip = 1              # Number of ASCII characters to skip after a unicode character.
   curskip = 0             # Number of ASCII characters left to skip
   out = []                # Output buffer.
   for match in pattern.finditer(text):
      word,arg,hex,char,brace,tchar = match.groups()
      if brace:
         curskip = 0
         if brace == '{':
            # Push state
            stack.append((ucskip,ignorable))
         elif brace == '}':
            # Pop state
            ucskip,ignorable = stack.pop()
      elif char: # \x (not a letter)
         curskip = 0
         if char == '~':
            if not ignorable:
                out.append('\xA0')
         elif char in '{}\\':
            if not ignorable:
               out.append(char)
         elif char == '*':
            ignorable = True
      elif word: # \foo
         curskip = 0
         if word in destinations:
            ignorable = True
         elif ignorable:
            pass
         elif word in specialchars:
            out.append(specialchars[word])
         elif word == 'uc':
            ucskip = int(arg)
         elif word == 'u':
            c = int(arg)
            if c < 0: c += 0x10000
            if c > 127: out.append(chr(c)) #NOQA
            else: out.append(chr(c))
            curskip = ucskip
      elif hex: # \'xx
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            c = int(hex,16)
            if c > 127: out.append(chr(c)) #NOQA
            else: out.append(chr(c))
      elif tchar:
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            out.append(tchar)
   return ''.join(i.decode('latin-1', 'replace') for i in out)



def striprtfhtml(text):
   pattern = re.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", re.I)
   # control words which specify a "destionation".
   destinations = frozenset((
      'aftncn','aftnsep','aftnsepc','annotation','atnauthor','atndate','atnicn','atnid',
      'atnparent','atnref','atntime','atrfend','atrfstart','author','background',
      'bkmkend','bkmkstart','blipuid','buptim','category','colorschememapping',
      'colortbl','comment','company','creatim','datafield','datastore','defchp','defpap',
      'do','doccomm','docvar','dptxbxtext','ebcend','ebcstart','factoidname','falt',
      'fchars','ffdeftext','ffentrymcr','ffexitmcr','ffformat','ffhelptext','ffl',
      'ffname','ffstattext','field','file','filetbl','fldinst','fldrslt','fldtype',
      'fname','fontemb','fontfile','fonttbl','footer','footerf','footerl','footerr',
      'footnote','formfield','ftncn','ftnsep','ftnsepc','g','generator','gridtbl',
      'header','headerf','headerl','headerr','hl','hlfr','hlinkbase','hlloc','hlsrc',
      'hsv','htmltag','info','keycode','keywords','latentstyles','lchars','levelnumbers',
      'leveltext','lfolevel','linkval','list','listlevel','listname','listoverride',
      'listoverridetable','listpicture','liststylename','listtable','listtext',
      'lsdlockedexcept','macc','maccPr','mailmerge','maln','malnScr','manager','margPr',
      'mbar','mbarPr','mbaseJc','mbegChr','mborderBox','mborderBoxPr','mbox','mboxPr',
      'mchr','mcount','mctrlPr','md','mdeg','mdegHide','mden','mdiff','mdPr','me',
      'mendChr','meqArr','meqArrPr','mf','mfName','mfPr','mfunc','mfuncPr','mgroupChr',
      'mgroupChrPr','mgrow','mhideBot','mhideLeft','mhideRight','mhideTop','mhtmltag',
      'mlim','mlimloc','mlimlow','mlimlowPr','mlimupp','mlimuppPr','mm','mmaddfieldname',
      'mmath','mmathPict','mmathPr','mmaxdist','mmc','mmcJc','mmconnectstr',
      'mmconnectstrdata','mmcPr','mmcs','mmdatasource','mmheadersource','mmmailsubject',
      'mmodso','mmodsofilter','mmodsofldmpdata','mmodsomappedname','mmodsoname',
      'mmodsorecipdata','mmodsosort','mmodsosrc','mmodsotable','mmodsoudl',
      'mmodsoudldata','mmodsouniquetag','mmPr','mmquery','mmr','mnary','mnaryPr',
      'mnoBreak','mnum','mobjDist','moMath','moMathPara','moMathParaPr','mopEmu',
      'mphant','mphantPr','mplcHide','mpos','mr','mrad','mradPr','mrPr','msepChr',
      'mshow','mshp','msPre','msPrePr','msSub','msSubPr','msSubSup','msSubSupPr','msSup',
      'msSupPr','mstrikeBLTR','mstrikeH','mstrikeTLBR','mstrikeV','msub','msubHide',
      'msup','msupHide','mtransp','mtype','mvertJc','mvfmf','mvfml','mvtof','mvtol',
      'mzeroAsc','mzeroDesc','mzeroWid','nesttableprops','nextfile','nonesttables',
      'objalias','objclass','objdata','object','objname','objsect','objtime','oldcprops',
      'oldpprops','oldsprops','oldtprops','oleclsid','operator','panose','password',
      'passwordhash','pgp','pgptbl','picprop','pict','pn','pnseclvl','pntext','pntxta',
      'pntxtb','printim','private','propname','protend','protstart','protusertbl','pxe',
      'result','revtbl','revtim','rsidtbl','rxe','shp','shpgrp','shpinst',
      'shppict','shprslt','shptxt','sn','sp','staticval','stylesheet','subject','sv',
      'svb','tc','template','themedata','title','txe','ud','upr','userprops',
      'wgrffmtfilter','windowcaption','writereservation','writereservhash','xe','xform',
      'xmlattrname','xmlattrvalue','xmlclose','xmlname','xmlnstbl',
      'xmlopen',
   ))
   # Translation of some special characters.
   specialchars = {
      'par': '<br>',
      'sect': '<br>',
      'page': '<br><br>',
      'line': '<br>',
      'tab': ' | ',
      'cell': ' | ',
      'row': ' |<br>',
      'emdash': '\u2014',
      'endash': '\u2013',
      'emspace': '\u2003',
      'enspace': '\u2002',
      'qmspace': '\u2005',
      'bullet': '\u2022',
      'lquote': '\u2018',
      'rquote': '\u2019',
      'ldblquote': '\201C',
      'rdblquote': '\u201D',
   }
   stack = []
   ignorable = False       # Whether this group (and all inside it) are "ignorable".
   ucskip = 1              # Number of ASCII characters to skip after a unicode character.
   curskip = 0             # Number of ASCII characters left to skip
   out = []                # Output buffer.
   for match in pattern.finditer(text):
      word,arg,hex,char,brace,tchar = match.groups()
      if brace:
         curskip = 0
         if brace == '{':
            # Push state
            stack.append((ucskip,ignorable))
         elif brace == '}':
            # Pop state
            ucskip,ignorable = stack.pop()
      elif char: # \x (not a letter)
         curskip = 0
         if char == '~':
            if not ignorable:
                out.append('\xA0')
         elif char in '{}\\':
            if not ignorable:
               out.append(char)
         elif char == '*':
            ignorable = True
      elif word: # \foo
         curskip = 0
         if word in destinations:
            ignorable = True
         elif ignorable:
            pass
         elif word in specialchars:
            out.append(specialchars[word])
         elif word == 'uc':
            ucskip = int(arg)
         elif word == 'u':
            c = int(arg)
            if c < 0: c += 0x10000
            if c > 127: out.append(chr(c)) #NOQA
            else: out.append(chr(c))
            curskip = ucskip
      elif hex: # \'xx
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            c = int(hex,16)
            if c > 127: out.append(chr(c)) #NOQA
            else: out.append(chr(c))
      elif tchar:
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            out.append(tchar)
   return ''.join(i.decode('latin-1', 'replace') for i in out)