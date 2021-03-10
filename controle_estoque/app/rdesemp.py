#!/usr/bin/python
# -*- coding: utf-8 -*-
import itertools

import os
import sys
import cherrypy
# cherrypy.process.plugins.Daemonizer(cherrypy.engine).subscribe()
import locale

from sqlalchemy.ext.declarative.api import declared_attr
#from typing import Dict, List, Any, Union
from sqlalchemy.sql.functions import concat

import contesputil
import json
import decimal
import datetime
import extenso
import sql_param
import ctplanpresum

import comercial

from auth import AuthController, require, member_of, name_is, nivel

thisdir = os.path.dirname(os.path.abspath(__file__))
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(thisdir + '/static'))

env.filters['currency'] = contesputil.format_currency_locale
env.filters['dinusa'] = contesputil.format_currency
env.globals['hora_atual'] = contesputil.hora_atual
env.filters['formatdate'] = contesputil.formatDate
env.filters['formathtml'] = contesputil.formathtml
env.globals['geterros'] = contesputil.geterros
env.filters['striprtf'] = contesputil.striprtf
env.filters['striprtfhtml'] = contesputil.striprtfhtml

import simplejson


def jsonp(func):
    def foo(self, *args, **kwargs):
        callback, _ = None, None
        if 'callback' in kwargs and '_' in kwargs:
            callback, _ = kwargs['callback'], kwargs['_']
            del kwargs['callback'], kwargs['_']
        ret = func(self, *args, **kwargs)
        if callback is not None:
            ret = '%s(%s)' % (callback, simplejson.dumps(ret))
        return ret

    return foo


class qtdlanc:
    def __init__(self, nome):
        self.nome = nome
        self.entrada = 0
        self.saida = 0
        self.servico = 0
        self.cupon = 0
        self.confere = 0
        self.cliente = 0

    def total(self):
        return self.entrada + self.saida + self.servico + self.cupon


class rel_desemp:
    auth = AuthController()

    cml = comercial.comercial_metas()

    def lista_userlst(self):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select usuario.iduser, usuario.nome , usuario.nomcompleto from usuario
        where usuario.status='N'
        order by usuario.nome '''
        cursorfb.execute(sql.encode('ascii'))
        return contesputil.dictcursor(cursorfb)

    def lista_user(self):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,usuario.depto , depto.id_depto ,usuario.ramal,usuario.email,usuario.path_foto,usuario.nivel
        ,usuario.encarregado ,usuario.funcao ,usuario.dat_inicio
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,current_date) gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join depto on depto.nomedepartamento = usuario.depto
        where usuario.status='N'
        order by usuario.nome '''
        cursorfb.execute(sql.encode('ascii'))
        return contesputil.dictcursor(cursorfb)

    def lista_eqp(self):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select grupousr.id as grp_id, grupousr.descricao as grp_nome , ID_DEPTO
        from grupousr
        order by grupousr.descricao '''
        cursorfb.execute(sql.encode('ascii'))

        return contesputil.dictcursor2(cursorfb)

    def lista_tempo_user(self, nmuser, setor, dt_ini):
        lst_tempo = list()
        dat_compt = dt_ini
        for mes in range(6):
            tempo_user = dict()
            lst_tempo.append(tempo_user)
            tempo_user['nome'] = nmuser
            tempo_user['data'] = dat_compt
            tempo_user['qtd'] = 0
            tempo_user['tempo'] = 0
            datafinal = contesputil.add_one_month(dat_compt)
            strdatafim = datafinal.strftime("%d.%m.%Y")
            strdataini = dat_compt.strftime("%d.%m.%Y")
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''select * from desempenho_tempo_user('%(dtini)s','%(dtfim)s', %(setor)s ,'%(nome)s') 
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': setor, 'nome': nmuser}
            cursorfb.execute(sql.encode('ascii'))
            desemp_user = contesputil.dictcursor(cursorfb)
            if desemp_user:
                tempo_user['qtd'] = desemp_user[0]['QTD']
                tempo_user['tempo'] = desemp_user[0]['TEMPO']
            dat_compt = contesputil.subtract_one_month(dat_compt)
        return lst_tempo

    def lista_eqp_usr(self, grp_id, dt_ini):
        datainicial = dt_ini
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d.%m.%Y")
        strdataini = datainicial.strftime("%d.%m.%Y")
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select grupousr_participante_vig.id_grupousr as grp_id 
        ,usuario.iduser, usuario.nome , usuario.nomcompleto 
        ,usuario.depto , depto.id_depto ,usuario.email,usuario.path_foto,usuario.nivel
        ,usuario.encarregado ,usuario.funcao ,usuario.dat_inicio
        ,usuario.hrs_meta ,coalesce(desconto.tempodesc,0) tmp_desc
        ,coalesce(desconto.meta,0) tmp_meta
        from grupousr_participante_vig
        left join usuario on grupousr_participante_vig.id_usuario=usuario.iduser
        left join depto on depto.nomedepartamento = usuario.depto
        left join CALCULA_USR_META_RET2('%(pcomp)s',usuario.iduser) as desconto on desconto.iduser=usuario.iduser
        where grupousr_participante_vig.id_grupousr = %(gid)s
        and grupousr_participante_vig.vigencia in
        (
        select max(vigencia) from grupousr_participante_vig
         where grupousr_participante_vig.id_grupousr = %(gid)s
          and grupousr_participante_vig.vigencia <= '%(pcomp)s'
        )
        order by usuario.nome ''' % {'gid': grp_id, 'pcomp': strdataini}
        cursorfb.execute(sql.encode('ascii'))
        lst_usr = contesputil.dictcursor2(cursorfb)
        if lst_usr:
            hj = datetime.date.today()
            if (datainicial.month == hj.month) and (datainicial.year == hj.year):
                diasfinal = hj
            else:
                diasfinal = contesputil.ultimodia(datainicial).date()
            numdepto = lst_usr[0]['ID_DEPTO']
            cursorfb = contesputil.ret_cursor('fb')
            sql = ''' select * from DESEMPENHO_USER('%(dtini)s','%(dtfim)s', %(setor)s )
                ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
            cursorfb.execute(sql.encode('ascii'))
            lst_metas = contesputil.dictcursor(cursorfb)
            for us in lst_usr:
                us['tempo_total'] = next((t['TOTAL'] for t in lst_metas if t['NOME'] == us['NOME']), None)
                tempo = (us['HRS_META'] or 0.0)
                tempo_desc = (us['TMP_DESC'] or 0)

                tmp_gasto = (us['tempo_total'] or 0) / 60
                us['hrs_trab'] = tmp_gasto
                us['meta'] = int((us['TMP_META'] or 0) / 60)
                us['qtd'] = self.busca_qtd(us['NOME'], strdataini, numdepto)
                us['lstqtd'] = self.busca_qtdcli(us['NOME'], strdataini, numdepto)
        return lst_usr

    def lista_eqp_cli(self, grp_id, dt_ini):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select GRUPOUSR_CLIENTE.id_grupousr as grp_id ,GRUPOUSR_CLIENTE.idCLIENTE ,
        cliente.codigo,cliente.razao,cliente.status
        from GRUPOUSR_CLIENTE
        left join CLIENTE on cliente.codigo = grupousr_cliente.idcliente
        where grupousr_cliente.id_grupousr = %(gid)s
        order by cliente.razao ''' % {'gid': grp_id}
        cursorfb.execute(sql.encode('ascii'))
        lst_cli = contesputil.dictcursor2(cursorfb)
        datainicial = dt_ini
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d.%m.%Y")
        strdataini = datainicial.strftime("%d.%m.%Y")

        # Pega total tempo por cliente
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select * from desempenho_cli_setor('%(dtini)s','%(dtfim)s' )
            ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cursorfb.execute(sql.encode('ascii'))
        cli_tempo = contesputil.dictcursor(cursorfb)

        # Pega total qtd por cliente contabil / fiscal
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select COD_CONTROLE,IDDEPTO , sum(qtd) as qtd from SERV_QTDLANC 
             where COMPETENCIA = '%(dtini)s'
             group by COD_CONTROLE,IDDEPTO 
            ''' % {'dtini': strdataini}
        cursorfb.execute(sql.encode('ascii'))
        cli_qtd = contesputil.dictcursor(cursorfb)

        # Carrega dados pela procedure
        for cli in lst_cli:
            cli['tempo_pessoal'] = next(
                (t['TOTAL'] for t in cli_tempo if ((t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 2))), 0) / 60
            cli['tempo_fiscal'] = next(
                (t['TOTAL'] for t in cli_tempo if (t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 3)), 0) / 60
            cli['tempo_contabil'] = next(
                (t['TOTAL'] for t in cli_tempo if (t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 4)), 0) / 60
            cli['tempo_total'] = cli['tempo_pessoal'] + cli['tempo_fiscal'] + cli['tempo_contabil']
            cli['qtd_pessoal'] = next(
                (t['QTD'] for t in cli_qtd if ((t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 2))), 0)
            cli['qtd_fiscal'] = next(
                (t['QTD'] for t in cli_qtd if (t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 3)), 0)
            cli['qtd_contabil'] = next(
                (t['QTD'] for t in cli_qtd if (t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 4)), 0)
        return lst_cli

    def lista_user2(self, dtini):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,coalesce(desconto.meta,0) tmp_meta
        ,coalesce(desconto.tempodesc,0) tmp_desc
        ,usuario.dat_inicio , usuario.dat_fim
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join CALCULA_USR_META_RET2('%(pcomp)s',usuario.iduser) as desconto on desconto.iduser=usuario.iduser
        where (('%(pcomp)s' <= dat_fim  ) or (dat_fim is null))
          and (('%(pcomp)s' >= dateadd( -1 month to dat_inicio)  ) or (dat_inicio is null))
        order by usuario.nome
        ''' % {'pcomp': dtini}
        cursorfb.execute(sql.encode('ascii'))
        lst_usr = contesputil.dictcursor(cursorfb)
        return lst_usr

    @cherrypy.expose
    def contr_ferias(self, **kwargs):
        envia_msg = 'N'
        if kwargs.has_key('envia_msg'):
            envia_msg = kwargs['envia_msg']
        envia_msg = 'S'
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select usuario.iduser,usuario.nome,usuario.depto,depto.USERRESP,
        usuario.nivel,usuario.cliente_dominio,usuario.empreg_dominio,usuario.LOGIN_DOMINIO,
        usuario.dat_inicio ,usuario.HRS_META from usuario
        left join depto on depto.NOMEDEPARTAMENTO=usuario.depto
         where usuario.status='N' order by usuario.nome'''
        cursorfb.execute(sql.encode('ascii'))
        dfbusers = contesputil.dictcursor(cursorfb)
        cursordo = contesputil.ret_cursor('do')
        sql = ''' SELECT codi_emp,i_empregados,nome as nm_empr,admissao,data_nascimento,venc_ferias,admissao+90-getdate() as term_exp, admissao+90 as dt_term FROM  bethadba.foempregados
            WHERE codi_emp in (45,565,780,966,5288 ) and i_afastamentos=1
            and tipo_contrib='X'
            order by nome
        '''
        cursordo.execute(sql.encode('ascii'))
        ddousers = contesputil.dictcursor2l(cursordo)
        for us in dfbusers:
            for usdo in ddousers:
                if (us['CLIENTE_DOMINIO'] == usdo['codi_emp']) and (us['EMPREG_DOMINIO'] == usdo['i_empregados']):
                    us.update(usdo)
        html = u'<html><table BORDER=1 > <thead><th>ID</th><th>Nome</th><th>Depto</th><th>Empresa</th><th>N.Empregado</th><th>Venc.Ferias</th><th>Admissão</th><th>Nascimento</th><th>Termino Experiencia</th></thead>'
        resumo = []
        for us in dfbusers:
            html += u'<tr>'
            html += u'<td> %s </td>' % us['IDUSER']
            html += u'<td> %s </td>' % us['NOME']
            html += u'<td> %s </td>' % unicode(us['DEPTO'], 'latin-1')
            html += u'<td> %s </td>' % us.get('CLIENTE_DOMINIO', 'vazio')
            html += u'<td> %s </td>' % us.get('EMPREG_DOMINIO', 'vazio')
            html += u'<td> %s </td>' % us.get('venc_ferias', 'vazio')
            html += u'<td> %s </td>' % us.get('admissao', 'vazio')
            html += u'<td> %s </td>' % us.get('data_nascimento', 'vazio')
            html += u'<td> %s </td>' % us.get('term_exp', 'vazio')
            if us.get('term_exp', 0) > 0:
                html += u'<td> %s </td>' % us.get('term_exp', 0)
            else:
                html += u'<td> Terminou </td>'
            html += u'</tr>'
            resdepto = next((x for x in resumo if x['DEPTO'] == us['DEPTO']), None)
            if resdepto is None:
                resdepto = {
                    'DEPTO': us['DEPTO'],
                    'RESP': us['USERRESP'],
                    'USUARIOS': [],
                    'MSG': ''
                }
                resumo.append(resdepto)
            if (us.get('term_exp', 0) > 0) and (us.get('NIVEL', 0) > 6):
                us_term_exp = us.get('term_exp', 0)
                dt_term_exp = us.get('dt_term', '0')
                resdepto['USUARIOS'].append({'NOME': us['NOME'], 'DIAS': us_term_exp, 'DT_FIM': dt_term_exp})
            if ((us.get('CLIENTE_DOMINIO', 'vazio') == 'vazio') or (us.get('admissao', 'vazio') == 'vazio')) and (
                    us.get('NIVEL', 0) > 6):
                resdepto['MSG'] += '\nVerificar cadastro do usuario %s codigo do empregado ' % str(us['NOME'])
            if (us.get('HRS_META', 0) == 0) and (us.get('NIVEL', 0) == 7):
                resdepto['MSG'] += '\nVerificar cadastro do usuario %s metas ' % str(us['NOME'])
            if (us.get('LOGIN_DOMINIO', '') == '') and (us.get('NIVEL', 0) > 6):
                resdepto['MSG'] += '\nVerificar cadastro do usuario %s Login do Dominio ' % str(us['NOME'])
        html += u'</table></html>'
        if cherrypy.request.remote.ip == '127.0.0.1':
            for dados_msg in resumo:
                if dados_msg['MSG'] != '':
                    sql = ''' insert into mensagem
                     (mensagem.aceitaexcluir
                         ,mensagem.assunto
                         ,mensagem.destinatario
                         ,mensagem.hora_fim , mensagem.hora_ini
                         ,mensagem.lida
                         ,mensagem.usuario
                         ,mensagem.msg)
                       values
                       (
                       'N','Verificar Cadastro dos Usuarios %(depto)s','LEANDRO_TI',
                       current_timestamp,current_timestamp,'N','SYSDBA',
                       '%(msg)s'
                       )
                    ''' % {'msg': dados_msg['MSG'], 'depto': dados_msg['DEPTO'].decode('latin-1').encode('ascii', 'replace')}
                    contesputil.execsql(sql.encode('ascii'), 'fb')
                    sql = ''' insert into mensagem
                     (mensagem.aceitaexcluir
                         ,mensagem.assunto
                         ,mensagem.destinatario
                         ,mensagem.hora_fim , mensagem.hora_ini
                         ,mensagem.lida
                         ,mensagem.usuario
                         ,mensagem.msg)
                       values
                       (
                       'N','Verificar Cadastro dos Usuarios %(depto)s','MAURICIO',
                       current_timestamp,current_timestamp,'N','SYSDBA',
                       '%(msg)s'
                       )
                    ''' % {'msg': dados_msg['MSG'], 'depto': dados_msg['DEPTO'].encode('ascii', 'replace')}
                    contesputil.execsql(sql.encode('ascii'), 'fb')
                if dados_msg['USUARIOS']:
                    dmsg = {'resp': dados_msg['RESP'],
                            'msg': dados_msg['RESP'] + '\nAviso termino de experiencia dos usuarios do ' + dados_msg[
                                'DEPTO'].encode('ascii', 'replace') + '\n\n'}
                    for duser in dados_msg['USUARIOS']:
                        dmsg['msg'] += 'Nome = ' + duser['NOME'] + ' > Faltam dias ' + str(
                            duser['DIAS']) + ' Termina dia ' + str(duser['DT_FIM']) + '\n'
                    sql = ''' insert into mensagem
                     (mensagem.aceitaexcluir
                         ,mensagem.assunto
                         ,mensagem.destinatario
                         ,mensagem.hora_fim , mensagem.hora_ini
                         ,mensagem.lida
                         ,mensagem.usuario
                         ,mensagem.msg)
                       values
                       (
                       'N','Aviso Termino de experiencia','LEANDRO_TI',
                       current_timestamp,current_timestamp,'N','SYSDBA',
                       '%(msg)s'
                       )
                    ''' % dmsg
                    contesputil.execsql(sql.encode('ascii'), 'fb')

                    sql = ''' insert into mensagem
                     (mensagem.aceitaexcluir
                         ,mensagem.assunto
                         ,mensagem.destinatario
                         ,mensagem.hora_fim , mensagem.hora_ini
                         ,mensagem.lida
                         ,mensagem.usuario
                         ,mensagem.msg)
                       values
                       (
                       'N','Aviso Termino de experiencia','%(resp)s',
                       current_timestamp,current_timestamp,'N','SYSDBA',
                       '%(msg)s'
                       )
                    ''' % dmsg
                    contesputil.execsql(sql.encode('ascii'), 'fb')
        return html

    def get_user_meta_dados(self, nmuser='', iduser=0, dtini=datetime.date.today()):
        strdataini = dtini.strftime("%d.%m.%Y")
        vnmuser = nmuser
        if iduser > 0:
            vnmuser = ''
        else:
            if vnmuser == '':
                vnmuser = '%'
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''
        select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,coalesce(desconto.meta,0) tmp_meta
        ,coalesce(desconto.tempodesc,0) tmp_desc
        ,usuario.dat_inicio , usuario.dat_fim
        ,usuario.depto , depto.id_depto ,usuario.ramal,usuario.email,usuario.path_foto,usuario.nivel
        ,usuario.encarregado ,usuario.funcao
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join CALCULA_USR_META_RET2('%(pcomp)s',usuario.iduser) as desconto on desconto.iduser=usuario.iduser
        left join depto on depto.nomedepartamento = usuario.depto
        where usuario.nome like '%(nmuser)s'
         or usuario.iduser = %(iduser)s
        order by usuario.nome
         ''' % {'pcomp': strdataini, 'nmuser': vnmuser, 'iduser': iduser}
        cursorfb.execute(sql.encode('ascii'))
        dsmetas = contesputil.dictcursor2(cursorfb)
        return dsmetas

    def get_user_meta(self, nmuser, dtini):
        vlr_meta = 0
        hrsdescontar = 0
        strdataini = dtini.strftime("%d.%m.%Y")
        hj = datetime.date.today()

        cursorfb = contesputil.ret_cursor('fb')
        sql = '''
        select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,coalesce(desconto.meta,0) tmp_meta
        ,coalesce(desconto.tempodesc,0) tmp_desc
        ,usuario.dat_inicio , usuario.dat_fim
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join CALCULA_USR_META_RET2('%(pcomp)s',usuario.iduser) as desconto on desconto.iduser=usuario.iduser
        where usuario.nome = '%(nmuser)s'
        order by usuario.nome
         ''' % {'pcomp': strdataini, 'nmuser': nmuser}
        cursorfb.execute(sql.encode('ascii'))

        dsmetas = contesputil.dictcursor(cursorfb)
        hrsmetauser = 0
        if dsmetas[0]['TMP_META']:
            vlr_meta = (dsmetas[0]['TMP_META'] / 60)
        return vlr_meta

    def lista_user_nm(self, dtini, nome):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,coalesce(desconto.tempodesc,0) tmp_desc
        ,coalesce(desconto.meta,0) tmp_meta
        ,usuario.dat_inicio , usuario.dat_fim
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join CALCULA_USR_META_RET2('%(pcomp)s',usuario.iduser) as desconto on desconto.iduser=usuario.iduser
        where usuario.nome='%(nome)s'
        order by usuario.nome ''' % {'nome': nome, 'pcomp': dtini}
        cursorfb.execute(sql.encode('ascii'))

        return contesputil.dictcursor(cursorfb)

    @cherrypy.expose
    def index(self):
        tmpl = env.get_template('inicio.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def qtdlfiscal(self):
        tmpl = env.get_template('qtdlfiscal.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def qtdlfiscal2(self):
        tmpl = env.get_template('qtdlfiscal2.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def qtdlcontabil(self):
        tmpl = env.get_template('qtdlcontabil.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totpuser(self):
        tmpl = env.get_template('totpuser.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totpuser2(self):
        tmpl = env.get_template('totpuser2.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totdepto(self):
        tmpl = env.get_template('totdepto.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totpequipe(self):
        tmpl = env.get_template('totpequipe.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totpequipe2(self):
        tmpl = env.get_template('totpequipe2.html')
        return tmpl.render(salutation='Hello', target='World', session=cherrypy.session)

    @cherrypy.expose
    def totproc(self):
        tmpl = env.get_template('totpprocesso.html')
        return tmpl.render(salutation='Hello', session=cherrypy.session)

    @cherrypy.expose
    def totcli(self):
        tmpl = env.get_template('totpcli.html')
        return tmpl.render(salutation='Hello', session=cherrypy.session)

    @cherrypy.expose
    def qtdlanctmp(self):
        tmpl = env.get_template('qtdlanctmp.html')
        return tmpl.render(salutation='Hello', session=cherrypy.session)

    @cherrypy.expose
    @require(nivel(9))
    def uploadapontamentoform(self):
        return """
        <html><body>
            <h2>Carregar arquivo de apontamentos</h2>
            <form action="uploadapontamento" method="post" enctype="multipart/form-data">
            <input type="file" name="myFile" /><br />
            <input type="submit" />
            </form>
        </body></html>
        """

    @require(nivel(9))
    @cherrypy.expose
    def uploadapontamento(self, myFile):
        out = """<html>
            <body>
                Tamanho do Arquivo: %s<br />
                Nome Arquivo: %s<br />
                Tipo Arquivo: %s<br />
                <h3>Dados Importados</h3><br>
                %s
            </body>
            </html>"""

        # Although this just counts the file length, it demonstrates
        # how to read large files in chunks instead of all at once.
        # CherryPy reads the uploaded file into a temporary file;
        # myFile.file.read reads from that.
        if str(myFile.content_type.value) != 'text/plain':
            return """<html><body>
            Tipo de arquivo invalido<br>
            Funcao somente recebe arquivo texto
            </body></html>
            """
        size = 0
        texto = ''
        dcodevento = {'0231': 'ATRASOS', '0040': 'FALTAS', '8701': 'ABONOS'}
        while True:
            data = myFile.file.read(8192)
            texto += data
            if not data:
                break
            size += len(data)
        nmcampos = ('empresa', 'empregado', 'dtocorencia', 'codevento', 'hrs', 'min')
        vcampos = (4, 8, 14, 18, 22, 24)
        drecebidos = list()
        for linha in texto.split('\n'):
            if len(linha) > 5:
                iantigo = 0
                dicdados = dict()
                for idx in range(len(vcampos)):
                    dicdados[nmcampos[idx]] = linha[iantigo:vcampos[idx]]
                    iantigo = vcampos[idx]
                dicdados['dados_empreg'] = sql_param.get_user_coddom(dicdados.get('empresa', 0),
                                                                     dicdados.get('empregado', 0))
                dicdados['evento'] = dcodevento.get(dicdados['codevento'], '')
                dicdados['usercad'] = cherrypy.session['NMUSER']
                if dicdados['dados_empreg']:
                    dicdados['inserido'] = sql_param.insere_apontamento(dicdados)
                else:
                    raise cherrypy.HTTPError(404, "Usuario nao encontrado codigo:" + dicdados.get('empregado', 0))
                drecebidos.append(dicdados)
        table = contesputil.table_html(drecebidos)
        return out % (size, myFile.filename, myFile.content_type, table)

    @cherrypy.expose
    def downftp(self, idarq):
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select  iddeposito,data_entrada,usuario,nmarquivo,conteudo, OCTET_LENGTH(conteudo)
             from deposito where iddeposito= %d ''' % int(idarq)
        cursorfb.execute(sql.encode('ascii'))
        #cursorfb.set_stream_blob('CONTEUDO')
        rs_arq = cursorfb.fetchone()
        arq_down = None
        nmarq = ''
        size = 0
        if rs_arq:
            print 'passa rs'
            print rs_arq
            nmarq = rs_arq[3]
            arq_down = rs_arq[4]
            size = rs_arq[5]

        
        # def stream():
        #	data = arq_down.read(BUF_SIZE)
        #	while len(data) > 0:
        #		yield data
        #		data = f.read(BUF_SIZE)
        # return stream()
        if size > 0:
            cherrypy.response.headers["Content-Type"] = "application/x-download"
            cherrypy.response.headers["Content-Disposition"] = 'attachment; filename="%s"' % nmarq
            cherrypy.response.headers["Content-Length"] = size
            BUF_SIZE = 1024 * 5
            return cherrypy.lib.static.serve_fileobj(arq_down, content_type="application/x-download",
                                                 disposition="attachment", name=nmarq)
        else:
            return 'Arquivo Vazio'


    @cherrypy.expose
    def ftp_contesp(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select codigo,razao from cliente where status='A' order by razao '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = u''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = u'selected'
            else:
                selec = u''
            htmlcli += u'<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                                'nome': cli_atual[1].decode('latin-1',
                                                                                                            'replace')}
        dtmpl['sel_cli'] = htmlcli
        dtmpl['scompt'] = selcompt
        htmlarq = ''
        if selcli > 0:
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''select deposito.iddeposito,deposito.data_entrada,deposito.usuario,deposito.nmarquivo,deposito.IDPROTOXOLO,protocolo2.IDIMPRESSAO,deposito.depto
             ,OCTET_LENGTH(conteudo) as tam 
             from deposito left join protocolo2 on protocolo2.idprot2=deposito.idprotoxolo where deposito.codcli=%(idcli)s
              and deposito.data_entrada >= '%(dtini)s'
              and deposito.data_entrada < '%(dtfim)s'
             order by deposito.data_entrada desc 
             ''' % {'idcli': selcli, 'dtini': strdataini, 'dtfim': strdatafim}
            cursorfb.execute(sql.encode('ascii'))
            rs_arq = cursorfb.fetchall()
            if rs_arq:
                htmlarq = u'<thead><tr><th>ID</th><th>Data</th><th>Usuario</th><th>Depto</th><th>Protocolo</th><th>Arquivo</th><th>Tamanho</th></tr>'
            for arq in rs_arq:
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % arq[0]
                htmlarq += u'<td> %s </td>' % arq[1]
                htmlarq += u'<td> %s </td>' % arq[2]
                htmlarq += u'<td> %s </td>' % unicode(contesputil.formathtml(arq[6]))
                htmlarq += u'<td><a href="detprot?id=%(id)s ">%(idimp)s </td>' % {'id': arq[4], 'idimp': arq[5]}
                htmlarq += u'<td><a href="downftp?idarq=%(idarq)s "> %(nome)s </a> </td>' % {'idarq': arq[0],
                                                                                             'nome': arq[3].decode(
                                                                                                 'latin-1', 'replace')}
                tambytes = arq[7]
                if isinstance(tambytes, int):
                    if tambytes > 1024:
                        tambytes = str(tambytes/1024)+' KB'
                htmlarq += u'<td> %s </td>' % tambytes

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('ftptmplt.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def lista_protserv(self, **kwargs):
        selserv = 0
        if kwargs.has_key('idserv'):
            selserv = int(kwargs['idserv']) or 0
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select idtpserv,nome||' - '||idtpserv from tpserv order by nome '''
        cursorfb.execute(sql.encode('ascii'))
        rs_serv = cursorfb.fetchall()
        htmlserv = ''
        for tpserv in rs_serv:
            if tpserv[0] == selserv:
                selec = 'selected'
            else:
                selec = ''
            htmlserv += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': tpserv[0], 'sel': selec,
                                                                                'nome': tpserv[1].decode('latin-1')}
        dtmpl['sel_serv'] = htmlserv
        dtmpl['scompt'] = selcompt
        htmlarq = ''
        if selserv > 0:
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''
            select servicos2.competencia,servicos2.venc,servicos2.QTD_ITEM,tpserv.idtpserv , protocolo2.idprot2,protocolo2.idimpressao,protocolo2.codclirep,protocolo2.cro, protocolo2.nome as razao,protocolo2.usuario ,protocolo2.status,protocolo2.emissao,protocolo2.entrega from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            inner join tpserv on tpserv.idtpserv=servicos2.idservico
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             AND SERVICOS2.idservico= %(idserv)s
            ORDER BY protocolo2.nome
             ''' % {'idserv': selserv, 'dtini': strdataini, 'dtfim': strdatafim}
            cursorfb.execute(sql.encode('ascii'))
            rs_serv = contesputil.dictcursor2(cursorfb)
            if rs_serv:
                htmlarq = u'<thead><tr><th>Cliente</th><th>Compt</th><th>Venc.</th><th>Qtd.</th><th>L.Protocolo</th><th>Data Emissao</th><th>Data Entrega</th><th>Usuario</th></tr>'
            for pserv in rs_serv:
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % pserv['RAZAO']
                htmlarq += u'<td> %s </td>' % pserv['COMPETENCIA']
                htmlarq += u'<td> %s </td>' % pserv['VENC']
                htmlarq += u'<td> %s </td>' % pserv['QTD_ITEM']
                htmlarq += u'<td><a href="detprot?id=%(idp)s "> %(nome)s </a> </td>' % {'idp': pserv['IDPROT2'],
                                                                                        'nome': pserv['IDPROT2']}
                htmlarq += u'<td> %s </td>' % pserv['EMISSAO']
                htmlarq += u'<td> %s </td>' % pserv['ENTREGA']
                htmlarq += u'<td> %s </td>' % pserv['USUARIO']

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('lista_protserv.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def entrg_impostos(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select codigo,razao from cliente where status='A' order by razao '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = ''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = 'selected'
            else:
                selec = ''
            htmlcli += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                               'nome': cli_atual[1].decode('latin-1')}
        dtmpl['sel_cli'] = htmlcli
        dtmpl['scompt'] = selcompt
        htmlarq = ''
        if selcli > 0:
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''
            select OBG.ID,CLASS.ID AS ID_CLASS,OBG.DESCR_OBRIG,DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC) AS DTA_VENC,ENTRG.DATENTREGA,ENTRG.NT,CLASS.SETOR,CLASS.DESCRICAO AS CLASS ,DEPTO.NM_SETOR
            FROM CKL_VENCOBRIG AS VENC
            INNER JOIN ckl_obrigcad AS OBG ON (VENC.ID_TPOBRIG=OBG.ID)
            INNER JOIN ckl_class AS CLASS ON (CLASS.ID=OBG.ID_CLASS)
            inner join CKL_OBRIGCLIENTE as CLI ON CLI.ID_TPOBRIG=VENC.ID_TPOBRIG AND CLI.VIGENCIA=VENC.COMPETENCIA
            LEFT JOIN CKL_ENTREGUE as ENTRG ON ENTRG.CODCLI=CLI.COD_CLI and ENTRG.CODIMP=VENC.ID_TPOBRIG AND ENTRG.VIG=VENC.COMPETENCIA
            left join depto on depto.id_depto=class.setor
            where (OBG.TP_VENC='M' or
            (OBG.TP_VENC='A' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int))) OR
            (OBG.TP_VENC='S' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 6)) OR
            (OBG.TP_VENC='T' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 3, cast(OBG.MES_ANUAL as int) + 6, cast(OBG.MES_ANUAL as int) + 9)))
            and DTA_VENC between '%(dtini)s' and '%(dtfim)s'
            and CLI.COD_CLI=%(idcli)s
            and CLASS.interno='N'
            ORDER BY DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC)
             ''' % {'idcli': selcli, 'dtini': strdataini, 'dtfim': strdatafim}
            cursorfb.execute(sql.encode('ascii'))
            impostos = contesputil.dictcursor3(cursorfb)

            if impostos:
                htmlarq = u'<thead><tr><th>ID</th><th>Setor</th><th>Imposto</th><th>Vencimento</th><th>Nao Teve</th><th>Entregue</th></tr>'
            for imp in impostos:
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % imp['ID']
                htmlarq += u'<td> %s </td>' % imp['NM_SETOR']
                htmlarq += u'<td> %s </td>' % imp['DESCR_OBRIG']
                htmlarq += u'<td> %s </td>' % imp['DTA_VENC']
                htmlarq += u'<td> %s </td>' % imp['NT']
                htmlarq += u'<td> %s </td>' % imp['DATENTREGA']

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('entrg_imp.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def calend_imp(self, **kwargs):
        selcli = 0
        callbk = ''
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('callback'):
            callbk = kwargs['callback']
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        calend = list()

        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select codigo,razao from cliente where status='A' order by razao '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = ''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = 'selected'
            else:
                selec = ''
            htmlcli += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                               'nome': cli_atual[1]}
        dtmpl['sel_cli'] = htmlcli
        dtmpl['scompt'] = selcompt
        htmlarq = ''
        if selcli > 0:
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''
            select OBG.ID,CLASS.ID AS ID_CLASS,OBG.DESCR_OBRIG,DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC) AS DTA_VENC,ENTRG.DATENTREGA,ENTRG.NT,CLASS.SETOR,CLASS.DESCRICAO AS CLASS ,DEPTO.NM_SETOR
            FROM CKL_VENCOBRIG AS VENC
            INNER JOIN ckl_obrigcad AS OBG ON (VENC.ID_TPOBRIG=OBG.ID)
            INNER JOIN ckl_class AS CLASS ON (CLASS.ID=OBG.ID_CLASS)
            inner join CKL_OBRIGCLIENTE as CLI ON CLI.ID_TPOBRIG=VENC.ID_TPOBRIG AND CLI.VIGENCIA=VENC.COMPETENCIA
            LEFT JOIN CKL_ENTREGUE as ENTRG ON ENTRG.CODCLI=CLI.COD_CLI and ENTRG.CODIMP=VENC.ID_TPOBRIG AND ENTRG.VIG=VENC.COMPETENCIA
            left join depto on depto.id_depto=class.setor
            where (OBG.TP_VENC='M' or
            (OBG.TP_VENC='A' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int))) OR
            (OBG.TP_VENC='S' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 6)) OR
            (OBG.TP_VENC='T' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 3, cast(OBG.MES_ANUAL as int) + 6, cast(OBG.MES_ANUAL as int) + 9)))
            and DTA_VENC between '%(dtini)s' and '%(dtfim)s'
            and CLI.COD_CLI=%(idcli)s
            and CLASS.interno='N'
            ORDER BY DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC)
             ''' % {'idcli': selcli, 'dtini': strdataini, 'dtfim': strdatafim}
            cursorfb.execute(sql.encode('ascii'))
            impostos = contesputil.dictcursor3(cursorfb)

            if impostos:
                htmlarq = u'<thead><tr><th>ID</th><th>Setor</th><th>Imposto</th><th>Vencimento</th><th>Nao Teve</th><th>Entregue</th></tr>'
            for imp in impostos:
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % imp['ID']
                htmlarq += u'<td> %s </td>' % imp['NM_SETOR']
                htmlarq += u'<td> %s </td>' % imp['DESCR_OBRIG']
                htmlarq += u'<td> %s </td>' % imp['DTA_VENC']
                htmlarq += u'<td> %s </td>' % imp['NT']
                htmlarq += u'<td> %s </td>' % imp['DATENTREGA']
                calend.append({'title': imp['DESCR_OBRIG'], 'start': imp['DTA_VENC'].strftime('%Y-%m-%d')})

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['session'] = cherrypy.session

        tmpl = env.get_template('entrg_imp.html')

        rt = ' %(cb)s ( %(js)s ) ' % {'cb': callbk, 'js': json.dumps(calend)}
        cherrypy.response.headers['Content-Type'] = 'application/javascript'
        cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
        print rt
        return rt


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def calend_imp2(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        calend = list()
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select codigo,razao from cliente where status='A' order by razao '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = ''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = 'selected'
            else:
                selec = ''
            htmlcli += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                               'nome': cli_atual[1]}
        dtmpl['sel_cli'] = htmlcli
        dtmpl['scompt'] = selcompt
        htmlarq = ''
        if selcli > 0:
            cursorfb = contesputil.ret_cursor('fb')
            sql = '''
            select OBG.ID,CLASS.ID AS ID_CLASS,OBG.DESCR_OBRIG,DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC) AS DTA_VENC,ENTRG.DATENTREGA,ENTRG.NT,CLASS.SETOR,CLASS.DESCRICAO AS CLASS ,DEPTO.NM_SETOR
            FROM CKL_VENCOBRIG AS VENC
            INNER JOIN ckl_obrigcad AS OBG ON (VENC.ID_TPOBRIG=OBG.ID)
            INNER JOIN ckl_class AS CLASS ON (CLASS.ID=OBG.ID_CLASS)
            inner join CKL_OBRIGCLIENTE as CLI ON CLI.ID_TPOBRIG=VENC.ID_TPOBRIG AND CLI.VIGENCIA=VENC.COMPETENCIA
            LEFT JOIN CKL_ENTREGUE as ENTRG ON ENTRG.CODCLI=CLI.COD_CLI and ENTRG.CODIMP=VENC.ID_TPOBRIG AND ENTRG.VIG=VENC.COMPETENCIA
            left join depto on depto.id_depto=class.setor
            where (OBG.TP_VENC='M' or
            (OBG.TP_VENC='A' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int))) OR
            (OBG.TP_VENC='S' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 6)) OR
            (OBG.TP_VENC='T' AND EXTRACT(MONTH FROM VENC.COMPETENCIA) in (cast(OBG.MES_ANUAL as int), cast(OBG.MES_ANUAL as int) + 3, cast(OBG.MES_ANUAL as int) + 6, cast(OBG.MES_ANUAL as int) + 9)))
            and DTA_VENC between '%(dtini)s' and '%(dtfim)s'
            and CLI.COD_CLI=%(idcli)s
            and CLASS.interno='N'
            ORDER BY DATEADD(OBG.NUM_DIA_VENC_INT DAY TO VENC.DTA_VENC)
             ''' % {'idcli': selcli, 'dtini': strdataini, 'dtfim': strdatafim}
            cursorfb.execute(sql.encode('ascii'))
            impostos = contesputil.dictcursor3(cursorfb)

            if impostos:
                htmlarq = u'<thead><tr><th>ID</th><th>Setor</th><th>Imposto</th><th>Vencimento</th><th>Nao Teve</th><th>Entregue</th></tr>'
            for imp in impostos:
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % imp['ID']
                htmlarq += u'<td> %s </td>' % imp['NM_SETOR']
                htmlarq += u'<td> %s </td>' % imp['DESCR_OBRIG']
                htmlarq += u'<td> %s </td>' % imp['DTA_VENC']
                htmlarq += u'<td> %s </td>' % imp['NT']
                htmlarq += u'<td> %s </td>' % imp['DATENTREGA']
                calend.append({'title': imp['DESCR_OBRIG']})
                calend.append({'start': unicode(imp['DTA_VENC'])})

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('entrg_imp.html')
        return calend

    @cherrypy.expose
    def calendreuniao(self, **kwargs):
        #print 'calend reuniao'
        respcalend = u''
        nmarq = 'reuniao.ics'
        import pytz
        from icalendar import Calendar, Event
        if not hasattr(self, 'dtlst_reuniao'):
            print ('dtlst_reuniao')
            self.dtlst_reuniao = datetime.datetime.now()
            self.lst_reuniao = sql_param.get_lstreuniao()
        dttmp = datetime.datetime.now() - self.dtlst_reuniao

        if dttmp.total_seconds() > 180.0:
            #print ('calc')
            self.dtlst_reuniao = datetime.datetime.now()
            self.lst_reuniao = sql_param.get_lstreuniao()
        cal = Calendar()
        cal.add('prodid', '-//Metas Contesp//')
        cal.add('version', '2.0')
        for evreuniao in self.lst_reuniao:
            event = Event()
            nminicio = evreuniao['HRINICIO'] - datetime.datetime(1899, 12, 30)
            nmfim = evreuniao['HRFIM'] - datetime.datetime(1899, 12, 30)
            event.add('uid', evreuniao['ID'])
            event.add('dtstart', evreuniao['DTA_REUNIAO'] + nminicio)
            event.add('dtend', evreuniao['DTA_REUNIAO'] + nmfim)
            event.add('dtstamp', evreuniao['DTA_REUNIAO'])
            event.add('summary', str(evreuniao['ID']) + ' -' + evreuniao['LOCAL'] + ' -' + evreuniao['NMUSER'])
            event.add('description',
                      evreuniao['NM_CLIENTE'] + '\n ' + 'Ref:' + evreuniao['REFERENTE'] + '\n Participantes:' +
                      evreuniao['LPARTICIPANTES'] + '\n ' + evreuniao['OBS'])
            event.add('categories', ['Reuniao'])
            cal.add_component(event)
        respcalend = cal.to_ical()

        return cherrypy.lib.static.serve_fileobj(respcalend, content_type="application/x-download",
                                                 disposition="attachment", name=nmarq)

    # return respcalend

    @cherrypy.expose
    def pontualidade_user(self, **kwargs):
        psetor = 0
        if kwargs.has_key('setor'):
            psetor = (int(kwargs['setor']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        tbl_html = u''
        if psetor > 0:
            if psetor == 2:
                dtmpl['seldp'] = 'selected'
            if psetor == 3:
                dtmpl['selfs'] = 'selected'
            if psetor == 4:
                dtmpl['selct'] = 'selected'
            if psetor == 26:
                dtmpl['selrh'] = 'selected'

            sql = '''select SERVICOS2.userrealiz as nome,prot_processo.descricao,tpserv.nome as servico,servicos2.venc,servicos2.atraso,servicos2.resp_atraso,count(1) AS qtd from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join prot_processo on prot_processo.id=tpserv.id_prot_process			
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico is not null
             and SERVICOS2.userrealiz is not null
             and prot_processo.id is not null
            group by SERVICOS2.userrealiz,prot_processo.descricao,tpserv.nome,servicos2.venc,servicos2.atraso,servicos2.resp_atraso
            order by SERVICOS2.userrealiz,prot_processo.descricao,tpserv.nome,servicos2.venc,servicos2.atraso,servicos2.resp_atraso
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            rowrs = cr.fetchall()
            if rowrs:
                tbl_html = u'<thead><tr><th>Usu&aacute;rio</th><th>Processo</th><th>Servi&ccedil;o</th><th>Vencimento</th><th>Atrasado</th><th>Resp. Atraso</th><th>Qtd</th></tr>'
            for serv in rowrs:
                tbl_html += u'<tr>'
                tbl_html += u'<td> %s </td>' % serv[0]
                tbl_html += u'<td> %s </td>' % serv[1].decode('latin-1')
                tbl_html += u'<td> %s </td>' % serv[2].decode('latin-1')
                tbl_html += u'<td> %s </td>' % serv[3]
                stratraso = '&nbsp;'
                if serv[4] == 'S':
                    stratraso = 'Sim'
                elif serv[4] == 'N':
                    stratraso = 'Nao'

                tbl_html += u'<td> %s </td>' % stratraso
                strresp = '&nbsp;'
                if serv[5] == 'C':
                    strresp = 'Cliente'
                elif serv[5] == 'F':
                    strresp = 'Contesp'

                tbl_html += u'<td> %s </td>' % strresp
                tbl_html += u'<td> %s </td>' % serv[6]
                tbl_html += u'</tr>'
        dtmpl['tbl_serv'] = tbl_html
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('pontuser.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def pontualidade_serv(self, **kwargs):
        psetor = 0
        if kwargs.has_key('setor'):
            psetor = (int(kwargs['setor']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        tbl_html = u''
        if psetor > 0:
            if psetor == 2:
                dtmpl['seldp'] = 'selected'
            if psetor == 3:
                dtmpl['selfs'] = 'selected'
            if psetor == 4:
                dtmpl['selct'] = 'selected'
            if psetor == 26:
                dtmpl['selrh'] = 'selected'

            sql = '''select prot_processo.descricao,tpserv.nome as servico,servicos2.atraso,servicos2.resp_atraso,count(1) AS qtd from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join prot_processo on prot_processo.id=tpserv.id_prot_process			
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico is not null
             and SERVICOS2.userrealiz is not null
             and prot_processo.id is not null
            group by prot_processo.descricao,tpserv.nome,servicos2.atraso,servicos2.resp_atraso
            order by prot_processo.descricao,tpserv.nome,servicos2.atraso,servicos2.resp_atraso
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            rowrs = cr.fetchall()
            if rowrs:
                tbl_html = u'<thead><th>Processo</th><th>Servi&ccedil;o</th><th>Atrasado</th><th>Resp. Atraso</th><th>Qtd</th></tr>'
            for serv in rowrs:
                tbl_html += u'<tr>'
                tbl_html += u'<td> %s </td>' % unicode(serv[0].decode('latin-1'))
                tbl_html += u'<td> %s </td>' % serv[1].decode('latin-1')
                stratraso = '&nbsp;'
                if serv[2] == 'S':
                    stratraso = 'Sim'
                elif serv[2] == 'N':
                    stratraso = 'Nao'

                tbl_html += u'<td> %s </td>' % stratraso
                strresp = '&nbsp;'
                if serv[3] == 'C':
                    strresp = 'Cliente'
                elif serv[3] == 'F':
                    strresp = 'Contesp'

                tbl_html += u'<td> %s </td>' % strresp
                tbl_html += u'<td> %s </td>' % serv[4]
                tbl_html += u'</tr>'
        dtmpl['tbl_serv'] = tbl_html
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('pontserv.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def pontualidade_proc(self, **kwargs):
        psetor = 0
        if kwargs.has_key('setor'):
            psetor = (int(kwargs['setor']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        tbl_html = u''
        if psetor > 0:
            if psetor == 2:
                dtmpl['seldp'] = 'selected'
            if psetor == 3:
                dtmpl['selfs'] = 'selected'
            if psetor == 4:
                dtmpl['selct'] = 'selected'
            if psetor == 26:
                dtmpl['selrh'] = 'selected'

            sql = '''select prot_processo.descricao,count(1) AS Total
                ,count(case when servicos2.atraso='N' then (1) end) as Nao_atrasado
                ,count(case when servicos2.atraso='S' then (1) end) as atrasado
                ,count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='C')) then (1) end) as atrasado_cliente
                ,count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='F')) then (1) end) as atrasado_contesp
                ,count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso IS NULL)) then (1) end) as atrasado_branco
             from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join prot_processo on prot_processo.id=tpserv.id_prot_process			
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico is not null
             and SERVICOS2.userrealiz is not null
            group by prot_processo.descricao
            order by prot_processo.descricao
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            rowrs = cr.fetchall()
            if rowrs:
                tbl_html = u'<thead><th>Processo</th><th>Total</th><th>Nao Atrasado</th><th>Atrasado</th><th>Atrasado Resp. Cliente</th><th>Atrasado Resp. Contesp</th><th>Atrasado Branco</th><th>Porcent. Pontualidade Resp. Contesp </th></tr>'
            for serv in rowrs:
                tbl_html += u'<tr>'
                tbl_html += u'<td> %s </td>' % (serv[0] or '').decode('latin-1')
                tbl_html += u'<td> %s </td>' % serv[1]
                tbl_html += u'<td> %s </td>' % serv[2]
                tbl_html += u'<td> %s </td>' % serv[3]
                tbl_html += u'<td> %s </td>' % serv[4]
                tbl_html += u'<td> %s </td>' % serv[5]
                tbl_html += u'<td> %s </td>' % serv[6]
                if serv[1] > 0:
                    porcent_atr = 100 - int((serv[5] / float(serv[1])) * 100)
                else:
                    porcent_atr = 0
                tbl_html += u'<td> %s </td>' % porcent_atr
                tbl_html += u'</tr>'
        dtmpl['tbl_serv'] = tbl_html
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('pontproc.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def pontualidade_proc2(self, **kwargs):
        psetor = 0
        if kwargs.has_key('setor'):
            psetor = (int(kwargs['setor']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        tbl_html = u''
        if psetor > 0:
            if psetor == 2:
                dtmpl['seldp'] = 'selected'
            if psetor == 3:
                dtmpl['selfs'] = 'selected'
            if psetor == 4:
                dtmpl['selct'] = 'selected'
            if psetor == 26:
                dtmpl['selrh'] = 'selected'
            ############################
            ###   PEGA O TOTAL POR PROCESSO
            sql = '''select prot_processo.id,prot_processo.descricao,count(1) AS Total
                ,count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='F')) then (1) end) as atrasado_contesp
                ,case
                 when (count(1) =0 ) then 0
                 else 100- cast ((   ((count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='F')) then (1) end))/(cast (count(1) as float))*100)) as integer)
                 end as PORCENT
             from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join prot_processo on prot_processo.id=tpserv.id_prot_process			
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico is not null
             and SERVICOS2.userrealiz is not null
             and prot_processo.proc_chave='S'
            group by prot_processo.id,prot_processo.descricao
            order by prot_processo.descricao
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            total_processos = contesputil.dictcursor(cr)
            ###########################################
            ###  PEGA TODOS OS PROCESSOS
            sql = '''select prot_processo.id,prot_processo.descricao
                ,prot_processo.objetivo_qualidade,prot_processo.indicador
                ,prot_processo.intrumento,prot_processo.meta
                ,prot_processo.frequencia,prot_processo.indice
            from prot_processo
            inner join depto on depto.nomedepartamento=prot_processo.depto
            where depto.id_depto = %(setor)s
            and prot_processo.proc_chave='S'
            order by prot_processo.descricao
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            descr_proc = contesputil.dictcursor2(cr)
            tbl_html = u'''<thead><tr>
                <th>OBJETIVOS DA QUALIDADE</th>
                <th>INDICADOR</th>
                <th>INSTRUMENTO</th>
                <th>FREQUENCIA</th>
                <th>INDICE</th>
                <th>META</th>
                <th>REAL</th>
                </tr></thead>'''
            # MONTA TABELA HTML
            for processo in descr_proc:
                lstvlr_real = [x for x in total_processos if x['ID'] == processo['ID']]
                if lstvlr_real:
                    vlr_real = lstvlr_real[0]['PORCENT']
                else:
                    vlr_real = 0

                tbl_html += '<tr>'
                tbl_html += '<td> %s </td>' % processo['OBJETIVO_QUALIDADE']
                tbl_html += '<td> %s </td>' % processo['INDICADOR']
                tbl_html += '<td> %s </td>' % processo['INTRUMENTO']
                tbl_html += '<td> %s </td>' % processo['FREQUENCIA']
                tbl_html += '<td> %s </td>' % processo['INDICE']
                tbl_html += '<td> %s </td>' % processo['META']
                tbl_html += '<td> %s </td>' % str(vlr_real)
                tbl_html += '</tr>'
        dtmpl['tbl_serv'] = tbl_html
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('pontproc.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def pontualidade_proc3(self, **kwargs):
        psetor = 0
        if kwargs.has_key('setor'):
            psetor = (int(kwargs['setor']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        tbl_html = u''
        if psetor > 0:
            if psetor == 2:
                dtmpl['seldp'] = 'selected'
            if psetor == 3:
                dtmpl['selfs'] = 'selected'
            if psetor == 4:
                dtmpl['selct'] = 'selected'
            if psetor == 26:
                dtmpl['selrh'] = 'selected'

            sql = '''select prot_processo.descricao,count(1) AS Total
                ,count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='F')) then (1) end) as atrasado_contesp
                ,case
                 when (count(1) =0 ) then 0
                 else 100- cast ((   ((count(case when ((servicos2.atraso='S') and (servicos2.resp_atraso='F')) then (1) end))/(cast (count(1) as float))*100)) as integer)
                 end
             from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join prot_processo on prot_processo.id=tpserv.id_prot_process			
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico is not null
             and SERVICOS2.userrealiz is not null
             and prot_processo.proc_chave='S'
            group by prot_processo.descricao
            order by prot_processo.descricao
            ''' % {'setor': psetor, 'dtini': strdataini, 'dtfim': strdatafim}
            cr = contesputil.ret_cursor('fb')
            print sql
            cr.execute(sql)
            rowrs = cr.fetchall()
            if rowrs:
                tbl_html = u'<thead><th>Processo</th><th>Total</th><th>Atrasado Resp. Contesp</th><th>Porcent</th><th>Porcent. Pontualidade Resp. Contesp </th></tr>'
            for serv in rowrs:
                tbl_html += u'<tr>'
                tbl_html += u'<td> %s </td>' % serv[0]
                tbl_html += u'<td> %s </td>' % serv[1]
                tbl_html += u'<td> %s </td>' % serv[2]
                tbl_html += u'<td> %s </td>' % serv[3]
                if serv[1] > 0:
                    porcent_atr = 100 - int((serv[2] / float(serv[1])) * 100)
                else:
                    porcent_atr = 0
                tbl_html += u'<td> %s </td>' % porcent_atr
                tbl_html += u'</tr>'
        dtmpl['tbl_serv'] = tbl_html
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('pontproc.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def det_equipe(self, **kwargs):
        pideq = 0
        if kwargs.has_key('ideqp'):
            pideq = (int(kwargs['ideqp']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        lst_eq = self.lista_eqp()
        dtmpl = dict()
        eqhtml = u''
        for eqp in lst_eq:
            selec = ''
            if pideq == eqp['GRP_ID']:
                selec = 'selected'
            eqhtml += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': eqp['GRP_ID'], 'sel': selec,
                                                                              'nome': eqp['GRP_NOME']}
        if pideq > 0:
            lst_equsr = self.lista_eqp_usr(pideq, datainicial)
            html = u'<thead><th>Nome</th><th>Tempo</th><th>Meta</th><th>Qtd</th><th>Qtd Cliente</th></thead></tr>'

            for us in lst_equsr:
                html += u'<tr>'
                html_link = '<a href="detuser?iduser=%(id_user)s&dt_ini=%(dini)s"> %(nome)s </a> ' % {
                    'nome': us['NOME'], 'id_user': us['IDUSER'], 'dini': selcompt}
                html += u'<td> %s </td>' % html_link
                html += u'<td> %s </td>' % us['hrs_trab']
                html += u'<td> %s </td>' % us['meta']
                html += u'<td> %s </td>' % us['qtd']
                htmltable = u'<table>'
                for cliqtd in us['lstqtd']:
                    htmltable += u'<tr><td> %(razao)s </td><td> %(qtd)s </td></tr>' % cliqtd
                htmltable += u'</table>'
                html += u'<td> %s </td>' % htmltable
                html += u'</tr>'
            dtmpl['tb_equsr'] = html
            html = u'<thead><Nome><Data><tempo></thead></tr>'
            html_categ = u''
            html_ser = u''
            lst_meta = list()
            for us in lst_equsr:
                us['lista_tempo'] = self.lista_tempo_user(us['NOME'], us['ID_DEPTO'], datainicial)
                nmserie = us['NOME']
                vlr_serie = ''
                for tempo in us['lista_tempo']:
                    meta = [t for t in lst_meta if t['data'] == tempo['data']]
                    if len(meta) > 0:
                        meta[0]['total'] += self.get_user_meta(us['NOME'], tempo['data'])
                    else:
                        meta = dict()
                        meta['data'] = tempo['data']
                        meta['total'] = self.get_user_meta(us['NOME'], tempo['data'])
                        lst_meta.append(meta)
                    html += u'<tr>'
                    html += u'<td> %s </td>' % tempo['nome']
                    html += u'<td> %s </td>' % tempo['data']
                    html += u'<td> %s </td>' % tempo['tempo']
                    html += u'</tr>'
                    vlr_serie += u' %s ,' % (tempo['tempo'] / 60)
                if html_categ == u'':
                    for tempo in us['lista_tempo']:
                        html_categ += u' "%s" ,' % tempo['data'].strftime("%m/%Y")
                html_ser += ''' {
                    type: 'column',
                    name : '%(nmser)s',
                    stacking: 'normal',
                    data : [%(dataser)s]
                }, ''' % {'nmser': nmserie, 'dataser': vlr_serie}
            vlr_meta = u''
            for meta in lst_meta:
                vlr_meta += u' %s ,' % (meta['total'])
            html_ser += ''' {
                    type: 'spline',
                    name : 'Meta',
                    
                    data : [%(dataser)s]
                } ''' % {'dataser': vlr_meta}

            dtmpl['tbl_lista_tempo'] = html
            dtmpl['tempo_cat'] = html_categ
            dtmpl['serie_chat'] = html_ser
            lst_eqcli = self.lista_eqp_cli(pideq, datainicial)
            html = u'''<thead><th>Razao</th><th>Tributa&ccedil;&atilde;o</th><th>Status</th>
                            </thead></tr>'''
            for cli in lst_eqcli:
                html += u'<tr>'
                html += u'<td> %s </td>' % cli['RAZAO']
                html += u'<td> %s </td>' % self.cliente_tributacao(cli['CODIGO'], datainicial)
                html += u'<td> %(status)s </td>' % {'status': cli['STATUS']}
                html += u'</tr>'
            dtmpl['tb_eqcli'] = html
        dtmpl['seleqp'] = eqhtml
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session

        tmpl = env.get_template('det_equipe.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def decl_livros_fiscais_netregues(self, **kwargs):
        dados_tmpl = dict()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1, month=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        if kwargs.has_key('sit'):
            dados_tmpl['selsit'] = int(kwargs['sit'])
        else:
            dados_tmpl['selsit'] = 0

        if kwargs.has_key('tipo'):
            dados_tmpl['seltipo'] = int(kwargs['tipo'])
        else:
            dados_tmpl['seltipo'] = 0

        selano = datainicial.year
        datafinal = datainicial.replace(year=selano + 1)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        tmpl = env.get_template('livros_decl.html')
        dados_tmpl['situacoes'] = [u'', u'Impresso',
                                   u'Enviado para cliente assinar',
                                   u'Enviado para encadernar',
                                   u'Encadernado',
                                   u'Enviado para registrar',
                                   u'Registrado',
                                   u'Enviado para cliente arquivar',
                                   u'Gerado para Encadernao',
                                   u'NT', u'Não Impresso']
        dados_tmpl['tipos'] = ['ENTRADA',
                               'SAIDA',
                               'ICMS',
                               'IPI',
                               'TOMADOS',
                               'PRESTADOS',
                               'INVENTARIO']
        dados_tmpl['scompt'] = selano
        dados_tmpl['session'] = cherrypy.session
        sql = '''select COALESCE( imp_fiscal.situacao,'') AS SITUACAO,'%(campo)s' AS TIPO, case
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '' then ''
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '-' then '-'
                 ELSE 'PREENCHIDO'
                 end as TP_VALOR
                ,count(1) as qtd from IMP_FISCAL
                where imp_fiscal.ano=%(ano)s
                group by imp_fiscal.situacao, case
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '' then ''
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '-' then '-'
                 ELSE 'PREENCHIDO'
                 end
                 ORDER BY imp_fiscal.situacao, case
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '' then ''
                 when coalesce(IMP_FISCAL.%(campo)s, '') = '-' then '-'
                 ELSE 'PREENCHIDO'
                 end
        ''' % {'ano': dados_tmpl['scompt'], 'campo': dados_tmpl['tipos'][dados_tmpl['seltipo'] - 1]}
        cr = contesputil.ret_cursor('fb')
        cr.execute(sql.encode('ascii'))
        dados_tmpl['resumo'] = contesputil.dictcursor3(cr)
        if dados_tmpl['seltipo'] > 0:
            sql = '''select IMP_FISCAL.*, CLIENTE.RAZAO from IMP_FISCAL
                    LEFT JOIN CLIENTE ON CLIENTE.CODIGO = IMP_FISCAL.COD_CLIENTE
                    where imp_fiscal.ano=%(ano)s
                     and coalesce(IMP_FISCAL.%(campo)s, '') = ''
                     and ((lower(imp_fiscal.situacao) = lower('%(sit)s')) or (imp_fiscal.situacao is null))
                    order by cliente.razao
            ''' % {'ano': dados_tmpl['scompt'], 'campo': dados_tmpl['tipos'][dados_tmpl['seltipo'] - 1],
                   'sit': dados_tmpl['situacoes'][dados_tmpl['selsit'] - 1]}
            cr = contesputil.ret_cursor('fb')
            cr.execute(sql.encode('latin-1'))
            dados_tmpl['detalhe'] = contesputil.dictcursor3(cr)
            dados_tmpl['sql'] = sql

        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def enviamsg(self, **kwargs):
        dados_tmpl = dict()
        if kwargs.has_key('emsg'):
            if kwargs['emsg'].strip() <> u'':
                # print kwargs['emsg']
                smsg = kwargs['emsg'].encode('latin-1')
                import socket, traceback
                import sys, time
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.bind(('', 5446))
                s.sendto(smsg, ('<broadcast>', 5446))
                dados_tmpl['msgenviada'] = kwargs['emsg']
                try:
                    sqlmsg = ''' insert into MSGTODOS (ORIGEM  ,MSG ) VALUES (?,?) '''
                    contesputil.execsqlp(sqlmsg, (
                        cherrypy.request.remote.ip,
                        smsg
                    ), 'fb')
                except:
                    print 'Erro no banco de dados'
        tmpl = env.get_template('enviamsg.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def enviamsgtst(self, **kwargs):
        dados_tmpl = dict()
        if kwargs.has_key('emsg'):
            if kwargs['emsg'].strip() <> u'':
                # print kwargs['emsg']
                smsg = kwargs['emsg'].encode('latin-1')
                import socket, traceback
                import sys, time
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.bind(('', 5446))
                s.sendto(smsg, ('192.168.0.220', 5446))
                dados_tmpl['msgenviada'] = kwargs['emsg']
                sqlmsg = ''' insert into MSGTODOS (ORIGEM  ,MSG ) VALUES (?,?) '''
                contesputil.execsqlp(sqlmsg, (
                    cherrypy.request.remote.ip,
                    smsg
                ), 'fb')
        tmpl = env.get_template('enviamsgtst.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def entrega_dirf(self, **kwargs):
        dados_tmpl = dict()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1, month=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        selano = datainicial.year
        datafinal = datainicial.replace(year=selano + 1)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        if kwargs.has_key('setor'):
            numdepto = kwargs['setor']
        else:
            numdepto = 0
        tbl_html = u''
        sql = '''select tpserv.nome,cliente.razao,protocolo2.idprot2,protocolo2.status
            ,protocolo2.responsavel2,protocolo2.entrega, servicos2.userrealiz,servicos2.qtd_item
            ,servicos2.competencia,servicos2.descravulsa
            from servicos2
        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
        inner join cliente on cliente.codigo=protocolo2.codclirep
        left join depto on depto.nomedepartamento=protocolo2.deptouser
        left join tpserv on tpserv.idtpserv=servicos2.idservico
        where protocolo2.emissao >= '%(dtini)s'
         and protocolo2.emissao < '%(dtfim)s'
         and protocolo2.status in ('B','N')
         and servicos2.idservico  in (109,488)
         and depto.id_depto = %(setor)s
        order by  tpserv.nome,cliente.razao,protocolo2.entrega
        ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cr = contesputil.ret_cursor('fb')

        cr.execute(sql)
        rowrs = cr.fetchall()
        if rowrs:
            tbl_html = u'<thead><th>Cliente</th><th>Protocolo</th><th>Para</th><th>Entrega</th><th>Usuario</th><th>Qtd</th><th>Comp.</th><th>Obs</th></tr>'
        for serv in rowrs:
            tbl_html += u'<tr>'
            tbl_html += u'<td> %s </td>' % contesputil.to_utf(serv[1])
            tbl_html += u'<td> %s </td>' % serv[2]
            tbl_html += u'<td> %s </td>' % unicode(contesputil.to_utf(serv[4]))
            tbl_html += u'<td> %s </td>' % serv[5]
            tbl_html += u'<td> %s </td>' % serv[6]
            tbl_html += u'<td> %s </td>' % serv[7]
            tbl_html += u'<td> %s </td>' % serv[8]
            tbl_html += u'<td> %s </td>' % serv[9]
            tbl_html += u'</tr>'
        dados_tmpl['scompt'] = selano

        dados_tmpl['tb_det'] = tbl_html
        dados_tmpl['setor'] = numdepto
        if numdepto == '2':
            dados_tmpl['seldp'] = 'selected'
        if numdepto == '3':
            dados_tmpl['selfs'] = 'selected'
        if numdepto == '4':
            dados_tmpl['selct'] = 'selected'
        tmpl = env.get_template('entrega_dirf.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    def getcombocli(self, codcli,filtraresp='S'):
        cursorfb = contesputil.ret_cursor('fb')
        if filtraresp =='S' :
            sqlfiltro = ''' and cliente.resp_financeiro='S' '''
        else:
            sqlfiltro = ''
        sql = '''select codigo,razao from cliente where status='A' %s order by razao ''' % sqlfiltro
        print sql
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = u''
        for cli_atual in rs_cli:
            if cli_atual[0] == codcli:
                selec = u'selected'
            else:
                selec = u''
            htmlcli += u'<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                                'nome': unicode(
                                                                                    cli_atual[1].decode('latin-1'))}
        return htmlcli

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def getcomboclijson(self, **kwargs):
        sfiltro = ''
        if kwargs.has_key('cod'):
            sfiltro += ' and codigo = '+str(kwargs.get('cod'))

        if kwargs.has_key('nmcli'):
            sfiltro += ''' and razao = '%s%%' ''' % str(kwargs.get('nmcli'))

        sql = '''select codigo,razao from cliente where status='A' %s  order by razao ''' % sfiltro
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        dados_resp = contesputil.dictcursor2(cursorfb)
        return dados_resp


    def getcomboclidom(self, codcli):
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT codi_emp as codigo,nome_emp as razao FROM bethadba.geempre emp where stat_emp='A' and codi_emp < 5000 order by nome_emp '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = u''
        for cli_atual in rs_cli:
            if cli_atual[0] == codcli:
                selec = u'selected'
            else:
                selec = u''
            htmlcli += u'<option value="%(cod)s" %(sel)s >%(nome)s - %(cod)s</option>' % {'cod': cli_atual[0],
                                                                                          'sel': selec, 'nome': unicode(
                    cli_atual[1].decode('utf-8'))}
        return htmlcli

    def getnmclidom(self, codcli):
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT codi_emp as codigo,nome_emp as razao FROM bethadba.geempre emp where stat_emp='A' and codi_emp = %s order by nome_emp ''' % str(
            codcli)
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = u''
        for cli_atual in rs_cli:
            htmlcli += unicode(cli_atual[1].decode('utf-8'))
        return htmlcli

    def getdesempcli(self, codcli, dtini, dtfim):
        cr_hrs = contesputil.ret_cursor('fb')
        sql = '''select desemp.* , usuario.hrs_meta ,(usuario.hrs_meta * 21) min_mes, depto.id_depto from desempenho_cli_user('%(pdtini)s','%(pdtfim)s',%(codcli)d )  as desemp
        left join usuario on usuario.iduser=desemp.iduser
        left join depto on depto.NOMEDEPARTAMENTO=usuario.depto
         ''' % {'pdtini': dtini.strftime('%d.%m.%Y'), 'pdtfim': dtfim.strftime('%d.%m.%Y'), 'codcli': codcli}
        cr_hrs.execute(sql.encode('ascii'))
        desemp_cli = contesputil.dictcursor3(cr_hrs)
        for duser in desemp_cli:
            duser['SALARIO_MES'] = 0.0
            duser['CODCLI'] = codcli
            if duser['CODIEMP'] > 0 and duser['CODIEMPRG'] > 0:
                sql = '''
                SELECT bethadba.fobasesserv.SALARIO_MES
                 FROM  bethadba.foempregados
                INNER JOIN  bethadba.fobasesserv ON bethadba.foempregados.CODI_EMP=bethadba.fobasesserv.CODI_EMP
                 AND bethadba.foempregados.I_EMPREGADOS=bethadba.fobasesserv.I_EMPREGADOS
                WHERE bethadba.foempregados.CODI_EMP = %(EMPRESA)s
                AND bethadba.foempregados.I_EMPREGADOS = %(EMPREGADO)s
                AND bethadba.fobasesserv.TIPO_PROCESS=11
                AND bethadba.fobasesserv.COMPETENCIA='%(dtini)s'
                AND bethadba.fobasesserv.rateio=1
                ORDER BY bethadba.foempregados.NOME
                ''' % {'dtini': dtini.strftime('%Y%m%d'), 'EMPRESA': duser['CODIEMP'], 'EMPREGADO': duser['CODIEMPRG']}
                cr = contesputil.ret_cursor('do')
                cr.execute(sql)
                if cr.description is not None:
                    sal = cr.fetchone()
                    if sal:
                        # for row in sal:
                        duser['SALARIO_MES'] = sal[0]
            duser['SALMINUT'] = 0.0
            duser['SMPART2'] = 0.0
            duser['SMPART3'] = 0.0
            duser['TEMPO_USER_TEMPO'] = 0
            duser['MIN_MES'] = float(duser.get('HRS_META', 0.0) or 0.0) * 21.0
            if duser['MIN_MES'] > 0.0:
                duser['SALMINUT'] = float(duser.get('SALARIO_MES', 0.0)) / duser['MIN_MES']
                porcent_contabilidade = 0
                if duser['CODIEMP'] == 45:
                    porcent_contabilidade = 27.27
                duser['SMPART2'] = (duser['SALMINUT'] * porcent_contabilidade / 100.0) + (duser['SALMINUT'] / 12.0) + (
                            duser['SALMINUT'] / 12.0 + (duser['SALMINUT'] / 12.0) / 3.0)
                duser['SMPART3'] = 362.20 / duser['MIN_MES']
            duser['CUSTO_MINUT'] = duser['SALMINUT'] + duser['SMPART2'] + duser['SMPART3']
            duser['CUSTO_TOTAL'] = duser['CUSTO_MINUT'] * duser['TOTAL']
        return desemp_cli

    def getdadosfincli(self, codcli, dtini):
        dados = dict()
        cr_cli = contesputil.ret_cursor('fb')
        sql = '''select cliente.codigo,cliente.razao,cliente.CNPJ,cliente.INSCEST,cliente.MAT_OU_FILIAL,cliente.RAMATIV,cliente.resp_financeiro
            ,coalesce(historico.valor,0) as vlr_mensalidade,fatura.nrfatura ,fatura.valor    as vlr_fatura 
            ,cliente.CLISERVCONT ,cliente.CLISERVFISCAL ,cliente.CLISERVDP ,cliente.CLISERVJURIDICO ,cliente.CLISERVINFOR ,cliente.CLISERVOUTROS 
            ,cliente.CLIPORCCONTABIL ,cliente.CLIPORCFISCAL ,cliente.CLIPORCDP ,cliente.CLIPORCJURIDICO ,cliente.CLIPORCINFOR ,cliente.CLIPORCOUTROS ,cliente.CLIPORCADM 
            from cliente
            left outer join fatura on fatura.codemp=cliente.codigo  and fatura.mesano = '%(pmesano)s' and fatura.avulso='N' and fatura.tprec='C'
            left outer join historico on fatura.nrfatura=historico.nrfatura and historico.codhist=1
            where cliente.codigo = %(pcodcli)s
         ''' % {'pmesano': dtini.strftime('%m%y'), 'pcodcli': codcli}
        cr_cli.execute(sql.encode('ascii'))
        if cr_cli.description is not None:
            dcli = cr_cli.fetchone()
            if dcli:
                descrel = cr_cli.description
                for col in range(len(descrel)):
                    if type(dcli[col]) == str:
                        dados[descrel[col][0]] = unicode(dcli[col].decode('latin-1'))
                    elif dcli[col] is None:
                        dados[descrel[col][0]] = unicode('')
                    else:
                        dados[descrel[col][0]] = dcli[col]
                # for row in sal:
                dados['MESANO'] = dtini.strftime('%m%y')
                if dados['RESP_FINANCEIRO'] == 'S':
                    cr_filial = contesputil.ret_cursor('fb')
                    sql = '''select cliente.codigo,cliente.razao,cliente.CNPJ,cliente.MAT_OU_FILIAL,cliente.RAMATIV from cliente
                        where cliente.CNPJ LIKE '%(pcnpj)s%%' and cliente.codigo <> %(pcod)d
                     ''' % {'pcnpj': dados['CNPJ'][:8], 'pcod': dados['CODIGO']}
                    cr_filial.execute(sql.encode('ascii'))
                    if cr_filial.description is not None:
                        dfiliais = contesputil.dictcursor3(cr_filial)
                        dados['FILIAIS'] = dfiliais
        return dados

    def getvlrclimensalidade(self, nrfatura):
        vlr = 0.0
        cr = contesputil.ret_cursor('fb')
        sql = ''' select sum (coalesce(historico.valor,0)) as valor from historico
         where historico.nrfatura='%(nrfat)s' and historico.codhist in ( 1,82) ''' % {'nrfat': nrfatura}
        cr.execute(sql)
        if cr.description is not None:
            hist = cr.fetchone()
            if hist:
                vlr = float(hist[0] or 0.0)
        return vlr

    def getvlrcliimp(self, nrfatura):
        vlr = 0.0
        cr = contesputil.ret_cursor('fb')
        sql = ''' select sum (coalesce(historico.valor,0)) as valor from historico
         where historico.nrfatura='%(nrfat)s' and historico.codhist in ( 55 ) ''' % {'nrfat': nrfatura}
        cr.execute(sql)
        if cr.description is not None:
            hist = cr.fetchone()
            if hist:
                vlr = float(hist[0] or 0.0)
        return vlr

    def getlistfincli(self, dtini):
        cr_cli = contesputil.ret_cursor('fb')
        dtfinal = contesputil.add_one_month(dtini)
        sql = '''select cliente.codigo,cliente.razao,cliente.CNPJ,cliente.MAT_OU_FILIAL,cliente.resp_financeiro
            ,fatura.nrfatura ,fatura.valor    as vlr_fatura 
            ,cliente.CLISERVCONT ,cliente.CLISERVFISCAL ,cliente.CLISERVDP ,cliente.CLISERVJURIDICO ,cliente.CLISERVINFOR ,cliente.CLISERVOUTROS 
            ,cliente.CLIPORCCONTABIL ,cliente.CLIPORCFISCAL ,cliente.CLIPORCDP ,cliente.CLIPORCJURIDICO ,cliente.CLIPORCINFOR ,cliente.CLIPORCOUTROS ,cliente.CLIPORCADM 
            from cliente
            inner  join fatura on fatura.codemp=cliente.codigo and fatura.avulso='N' and fatura.tprec='C'
            where fatura.fatpaga='S' and fatura.datpag >= '%(dtini)s' and fatura.datpag < '%(dtfim)s'
            order by cliente.codigo
         ''' % {'dtini': dtini.strftime('%d.%m.%Y'), 'dtfim': dtfinal.strftime('%d.%m.%Y')}
        cr_cli.execute(sql.encode('ascii'))
        desemp_cli = contesputil.dictcursor3(cr_cli)
        for fat in desemp_cli:
            fat['VLR_MENSALIDADE'] = self.getvlrclimensalidade(fat['NRFATURA'])
            fat['VLR_IMPOSTO'] = self.getvlrcliimp(fat['NRFATURA'])
        return desemp_cli

    @cherrypy.expose
    def getlistmsgtodos(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = kwargs['dt_ini'].replace('/', '.')
            selcompt = kwargs['dt_ini']
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
            selcompt = ini_mes.strftime('%d/%m/%Y')
        if kwargs.has_key('dt_fim'):
            strdatafim = kwargs['dt_fim'].replace('/', '.')
            selcomptfim = kwargs['dt_fim']
        else:
            ini_mes = datetime.date.today() + datetime.timedelta(days=1)
            strdatafim = ini_mes.strftime('%d.%m.%Y')
            selcomptfim = ini_mes.strftime('%d/%m/%Y')

        filtro = ''
        psq_palavra = ''
        if kwargs.has_key('psq_msg'):
            psq_palavra = kwargs['psq_msg']
            if psq_palavra != '':
                psq_palavra = psq_palavra.upper()
                filtro = ''' and upper(msg) like '%%%(palavra)s%%'  ''' % {'palavra': psq_palavra}

        cr_cli = contesputil.ret_cursor('fb')
        sql = '''SELECT * from MSGTODOS
            where data >= '%(dtini)s' and data <= '%(dtfim)s'
            %(filtro)s
            order by msgtodos.data 
         ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'filtro': filtro}
        print sql
        cr_cli.execute(sql)
        desemp_cli = contesputil.dictcursor3(cr_cli)

        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['scomptfim'] = selcomptfim
        dtmpl['spsq'] = psq_palavra
        dtmpl['session'] = cherrypy.session
        dtmpl['lstrel'] = desemp_cli
        tmpl = env.get_template('getlistmsgtodos.html')
        return tmpl.render(dtmpl)

    def gettotfincli(self, dtini):
        clis = self.getlistfincli(dtini)
        dados = {'VLRCLICONT': 0.0, 'VLRCLIFISC': 0.0, 'VLRCLIDP': 0.0,
                 'QTDCLICONT': 0, 'QTDCLIFISC': 0, 'QTDCLIDP': 0,
                 'VLRTOT': 0.0, 'QTDTOT': 0, 'VLRFAT': 0.0, 'VLRIMP': 0.0,
                 'VLRIMPCONT': 0.0, 'VLRIMPFISC': 0.0, 'VLRIMPDP': 0.0,
                 'VLRFATCONT': 0.0, 'VLRFATFISC': 0.0, 'VLRFATDP': 0.0}
        for cli in clis:
            dados['VLRTOT'] += float(cli.get('VLR_MENSALIDADE', 0.0))
            dados['VLRIMP'] += float(cli.get('VLR_IMPOSTO', 0.0))
            dados['VLRFAT'] += float(cli.get('VLR_FATURA', 0.0))
            dados['QTDTOT'] += 1
            if cli.get('CLISERVCONT', 'N') == 'S':
                dados['VLRCLICONT'] += float(cli.get('VLR_MENSALIDADE', 0.0))
                dados['VLRIMPCONT'] += float(cli.get('VLR_IMPOSTO', 0.0))
                dados['VLRFATCONT'] += float(cli.get('VLR_FATURA', 0.0))
                dados['QTDCLICONT'] += 1
            if cli.get('CLISERVFISCAL', 'N') == 'S':
                dados['VLRCLIFISC'] += float(cli.get('VLR_MENSALIDADE', 0.0))
                dados['VLRIMPFISC'] += float(cli.get('VLR_IMPOSTO', 0.0))
                dados['VLRFATFISC'] += float(cli.get('VLR_FATURA', 0.0))
                dados['QTDCLIFISC'] += 1
            if cli.get('CLISERVDP', 'N') == 'S':
                dados['VLRCLIDP'] += float(cli.get('VLR_MENSALIDADE', 0.0))
                dados['VLRIMPDP'] += float(cli.get('VLR_IMPOSTO', 0.0))
                dados['VLRFATDP'] += float(cli.get('VLR_FATURA', 0.0))
                dados['QTDCLIDP'] += 1
        return dados

    @cherrypy.expose
    def fct_lcont(self, **kwargs):
        selclidom = 0
        selcli = 0
        seladdfilial = 'S'
        hj = datetime.date.today()
        if kwargs.has_key('idcli'):
            selclidom = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('sfilial'):
            seladdfilial = str((kwargs['sfilial']) or 'S')
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            strdataini = '01.01.' + str(hj.year)
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        selcompt = str(datainicial.year)
        datafinal = datetime.datetime.strptime('01.01.' + str(datainicial.year + 1), '%d.%m.%Y')
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        dtmpl = dict()
        dtmpl['dtinicial'] = datainicial
        dtmpl['dtfinal'] = datafinal
        dtmpl['selclidom'] = selclidom
        lancamentos = list()

        if selclidom > 0:
            lstcodi_emp = sql_param.lst_cli_codi(selclidom, seladdfilial)
            dtmpl['lstcodi_emp'] = lstcodi_emp
            lancamentos = sql_param.get_ctlanc(lstcodi_emp, datainicial)
            lanctri = []

            for codi_emp in lstcodi_emp:
                lancamentos_cli = [l for l in lancamentos if l['codi_emp'] == codi_emp]

                lanctri.extend(sql_param.get_tottri(lancamentos_cli, codi_emp))
            dtmpl['ltri'] = lanctri

            lst_class = sql_param.get_lst_class()
            for cl in lst_class:
                cl['valor1'] = decimal.Decimal(0.0)
                cl['valor2'] = decimal.Decimal(0.0)
                cl['valor3'] = decimal.Decimal(0.0)
                cl['valor4'] = decimal.Decimal(0.0)
                cl['vipi1'] = decimal.Decimal(0.0)
                cl['vipi2'] = decimal.Decimal(0.0)
                cl['vipi3'] = decimal.Decimal(0.0)
                cl['vipi4'] = decimal.Decimal(0.0)
                cl['vst1'] = decimal.Decimal(0.0)
                cl['vst2'] = decimal.Decimal(0.0)
                cl['vst3'] = decimal.Decimal(0.0)
                cl['vst4'] = decimal.Decimal(0.0)
                for ac in lanctri:
                    if cl['ID_CLASS'] == ac['class']:
                        cl['valor1'] += ac['vlr_contabil0']
                        cl['valor2'] += ac['vlr_contabil1']
                        cl['valor3'] += ac['vlr_contabil2']
                        cl['valor4'] += ac['vlr_contabil3']
                        cl['vipi1'] += ac['vlr_ipi0']
                        cl['vipi2'] += ac['vlr_ipi1']
                        cl['vipi3'] += ac['vlr_ipi2']
                        cl['vipi4'] += ac['vlr_ipi3']
                        cl['vst1'] += ac['vlr_st0']
                        cl['vst2'] += ac['vlr_st1']
                        cl['vst3'] += ac['vlr_st2']
                        cl['vst4'] += ac['vlr_st3']
                porcent = cl['PORCENT_CTBASE'] or decimal.Decimal(0.0)
                if cl['SUBTRAIR'] == 'S':
                    cl['valor1'] *= -1
                    cl['valor2'] *= -1
                    cl['valor3'] *= -1
                    cl['valor4'] *= -1

                cl['base1'] = (cl['valor1'] - cl['vipi1'] - cl['vst1']) * porcent / decimal.Decimal(100)
                cl['base2'] = (cl['valor2'] - cl['vipi2'] - cl['vst2']) * porcent / decimal.Decimal(100)
                cl['base3'] = (cl['valor3'] - cl['vipi3'] - cl['vst3']) * porcent / decimal.Decimal(100)
                cl['base4'] = (cl['valor4'] - cl['vipi4'] - cl['vst4']) * porcent / decimal.Decimal(100)

            dtmpl['lclass'] = [cl for cl in lst_class if
                               cl['valor1'] <> decimal.Decimal(0.0) or cl['valor2'] <> decimal.Decimal(0.0) or cl[
                                   'valor3'] <> decimal.Decimal(0.0) or cl['valor4'] <> decimal.Decimal(0.0)]
            dtmpl['totipi'] = dict()
            dtmpl['totipi']['vipi1'] = sum([cl['vipi1'] for cl in dtmpl['lclass']])
            dtmpl['totipi']['vipi2'] = sum([cl['vipi2'] for cl in dtmpl['lclass']])
            dtmpl['totipi']['vipi3'] = sum([cl['vipi3'] for cl in dtmpl['lclass']])
            dtmpl['totipi']['vipi4'] = sum([cl['vipi4'] for cl in dtmpl['lclass']])
            dtmpl['totst'] = dict()
            dtmpl['totst']['vst1'] = sum([cl['vst1'] for cl in dtmpl['lclass']])
            dtmpl['totst']['vst2'] = sum([cl['vst2'] for cl in dtmpl['lclass']])
            dtmpl['totst']['vst3'] = sum([cl['vst3'] for cl in dtmpl['lclass']])
            dtmpl['totst']['vst4'] = sum([cl['vst4'] for cl in dtmpl['lclass']])
            dtmpl['totfat'] = dict()
            dtmpl['totfat']['valor1'] = sum([cl['valor1'] for cl in dtmpl['lclass']]) - dtmpl['totipi']['vipi1'] - \
                                        dtmpl['totst']['vst1']
            dtmpl['totfat']['valor2'] = sum([cl['valor2'] for cl in dtmpl['lclass']]) - dtmpl['totipi']['vipi2'] - \
                                        dtmpl['totst']['vst2']
            dtmpl['totfat']['valor3'] = sum([cl['valor3'] for cl in dtmpl['lclass']]) - dtmpl['totipi']['vipi3'] - \
                                        dtmpl['totst']['vst3']
            dtmpl['totfat']['valor4'] = sum([cl['valor4'] for cl in dtmpl['lclass']]) - dtmpl['totipi']['vipi4'] - \
                                        dtmpl['totst']['vst3']
            dtmpl['totbas'] = dict()
            dtmpl['totbas']['valor1'] = sum([cl['base1'] for cl in dtmpl['lclass']])
            dtmpl['totbas']['valor2'] = sum([cl['base2'] for cl in dtmpl['lclass']])
            dtmpl['totbas']['valor3'] = sum([cl['base3'] for cl in dtmpl['lclass']])
            dtmpl['totbas']['valor4'] = sum([cl['base4'] for cl in dtmpl['lclass']])
        dtmpl['lanc'] = lancamentos
        dtmpl['addfilial'] = seladdfilial
        dtmpl['sel_cli'] = self.getcomboclidom(selclidom)
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        # Calculando Impostos
        if selclidom > 0:
            dtmpl['limp'] = sql_param.somaimpostosct(dtmpl)
        tmpl = env.get_template('fct_lcont.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def avisa_protocolo_email(self, **kwargs):
        idprot = 0
        nmuser = None
        if kwargs.has_key('idprot'):
            idprot = int(kwargs['idprot']) or 0
        if kwargs.has_key('nmuser'):
            nmuser = kwargs['nmuser']
        if idprot <= 0:
            raise NameError('Numero do protocolo invalido')
        if kwargs.has_key('web'):
            contesputil.avisa_protocolo_onlinew(idprot, nmuser)
        else:
            contesputil.avisa_protocolo_online(idprot, nmuser)

    @cherrypy.expose
    def plct_presumido(self, **kwargs):
        selclidom = 0
        selcli = 0
        seladdfilial = 'S'
        selano = 2018
        hj = datetime.date(2018, 10, 1)
        if kwargs.has_key('idcli'):
            selclidom = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('sfilial'):
            seladdfilial = str((kwargs['sfilial']) or 'S')
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
            selano = int(kwargs.get('dt_ini', '0')) or 0
        else:
            strdataini = '01.01.' + str(hj.year)
            selano = hj.year
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        selcompt = str(datainicial.year)
        datafinal = datetime.datetime.strptime('01.01.' + str(datainicial.year + 1), '%d.%m.%Y')
        dtmpl = dict()
        dtmpl['dtinicial'] = datainicial
        dtmpl['dtfinal'] = datafinal
        dtmpl['selclidom'] = selclidom

        if selclidom > 0:
            lstcodi_emp = sql_param.lst_cli_codi(selclidom, seladdfilial)
            dtmpl['lstcodi_emp'] = lstcodi_emp
            ct = ctplanpresum.ctppres(selclidom, selano, seladdfilial)
            ct.carrega_lancamentos()
            dweb = ct.get_dict_web()
            dtmpl.update(dweb)
        print 'locale'
        print locale.localeconv()
        dtmpl['addfilial'] = seladdfilial
        dtmpl['sel_cli'] = self.getcomboclidom(selclidom)
        dtmpl['sel_nmcli'] = self.getnmclidom(selclidom)
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('planpresumido.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def add_ajust_pres(self, **kwargs):
        idcli = '0'
        tpimp = ''
        vlr = '0.01'
        compt = ''
        dados = cherrypy.request.json
        if dados.has_key('idcli'):
            idcli = dados['idcli']
        if dados.has_key('tpimposto'):
            tpimp = dados['tpimposto']
        if dados.has_key('vlrajuste'):
            vlr = dados['vlrajuste']
            vlr = vlr.replace('.', '')
            vlr = vlr.replace(',', '.')
        if dados.has_key('compt'):
            compt = '01.' + dados['compt'].replace('/', '.')
        pdados = {'idcli': idcli, 'compt': compt, 'tpimposto': tpimp, 'vlrajuste': vlr}
        print pdados
        print dados

        if idcli != '0':
            inssql = '''
            delete from FECH_AJUSTE_CONTABIL where CODI_EMP= ? and TIPO='P' AND COMPETENCIA = '%(compt)s' and IMPOSTO='%(tpimposto)s'
            ''' % pdados
            param = (idcli, vlr)
            contesputil.execsqlp(inssql, param, 'fb')
            inssql = '''
            insert into FECH_AJUSTE_CONTABIL ( CODI_EMP, TIPO, COMPETENCIA, IMPOSTO, VALOR)
            values ( ? ,'P' ,'%(compt)s' ,'%(tpimposto)s' , ? )
            ''' % pdados
            param = (idcli, vlr)
            contesputil.execsqlp(inssql, param, 'fb')
        selsql = '''
        select ID from FECH_AJUSTE_CONTABIL where CODI_EMP= %(idcli)s and TIPO='P' AND COMPETENCIA = '%(compt)s' and IMPOSTO='%(tpimposto)s'
        ''' % pdados
        cr_l = contesputil.ret_cursor('fb')
        cr_l.execute(selsql.encode('ascii'))
        dicl = contesputil.dictcursor3(cr_l)
        return dicl

    @cherrypy.expose
    def del_ajust_pres(self, **kwargs):
        idajs = '0'
        if kwargs.has_key('idajs'):
            idajs = kwargs['idajs']
        if idajs != '0':
            inssql = '''
            delete from FECH_AJUSTE_CONTABIL where id= %s ''' % str(idajs)
            contesputil.execsql(inssql, 'fb')
        return '1'

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def lst_ajust_pres(self, **kwargs):
        idcli = '0'
        tpimp = ''
        compt = ''
        dados = cherrypy.request.json
        if dados.has_key('idcli'):
            idcli = dados['idcli']
        if dados.has_key('tpimposto'):
            tpimp = dados['tpimposto']
        if dados.has_key('compt'):
            if dados['compt'] != '':
                compt = '01.' + dados['compt'].replace('/', '.')
            else:
                compt = ''
        pdados = {'idcli': idcli, 'compt': compt, 'tpimposto': tpimp}
        selsqltmp = " select * from FECH_AJUSTE_CONTABIL where CODI_EMP= %(idcli)s and TIPO='P' "
        if compt != '':
            selsqltmp += " AND COMPETENCIA = '%(compt)s' "
        if tpimp != '':
            selsqltmp += " and IMPOSTO='%(tpimposto)s'"
        selsql = selsqltmp + ' order by COMPETENCIA  '
        selsql = selsql % pdados
        print selsql
        cr_l = contesputil.ret_cursor('fb')
        cr_l.execute(selsql.encode('ascii'))
        dicl = contesputil.dictcursor2l(cr_l)
        return dicl

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def plct_presumido_det(self, **kwargs):
        selclidom = 0
        selcli = 0
        seladdfilial = 'S'
        selano = 0
        selidclass = None
        hj = datetime.date.today()
        if kwargs.has_key('idclass'):
            selidclass = kwargs['idclass']
            if selidclass:
                selidclass = int(selidclass)
        if kwargs.has_key('idcli'):
            selclidom = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('sfilial'):
            seladdfilial = str((kwargs['sfilial']) or 'S')
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
            selano = int(kwargs.get('dt_ini', '0')) or 0
        else:
            strdataini = '01.01.' + str(hj.year)
            selano = hj.year
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        selcompt = str(datainicial.year)
        datafinal = datetime.datetime.strptime('01.01.' + str(datainicial.year + 1), '%d.%m.%Y')
        dtmpl = dict()
        if selidclass:
            ct = ctplanpresum.ctppres(selclidom, selano, seladdfilial, selidclass)
            ct.carrega_lancamentos()
            dweb = ct.get_dict_json()
            dtmpl.update(dweb)
        return dtmpl

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def add_lembr_pres(self, **kwargs):
        dados = cherrypy.request.json
        codi_emp = dados.get('codi_emp', u'0')
        txtobs = dados.get('lembr', u'')
        if txtobs != u'':
            inssql = '''
            insert into FECH_CTLEMBRETE (CODI_EMP,TEXTO) values (?,?)
            '''
            param = (codi_emp, txtobs)
            contesputil.execsqlp(inssql, param, 'fb')
        selsql = '''
        select * from FECH_CTLEMBRETE where codi_emp = %s
        ''' % codi_emp
        cr_l = contesputil.ret_cursor('fb')
        cr_l.execute(selsql.encode('ascii'))
        dicl = contesputil.dictcursor3(cr_l)
        return dicl

    @cherrypy.expose
    def del_lembr_pres(self, **kwargs):
        print kwargs
        codi_l = kwargs.get('idlemb', u'0')
        if codi_l <> u'0':

            inssql = '''
            delete from FECH_CTLEMBRETE where id = %s
            ''' % codi_l
            param = tuple(codi_l)
            contesputil.execsqlp(inssql, param, 'fb')
            print inssql
        else:
            raise cherrypy.NotFound()
        return codi_l

    @cherrypy.expose
    @require(nivel(9))
    def treldebitos(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        if kwargs.has_key('userresp'):
            seluser = kwargs['userresp']
        else:
            seluser = cherrypy.session.get('RELDEBUSER', 'TODOS')
        if kwargs.has_key('selsit'):
            selsit = kwargs['selsit']
        else:
            selsit = cherrypy.session.get('RELDEBSELSIT', 'TODOS')
        cherrypy.session['RELDEBUSER'] = seluser
        cherrypy.session['RELDEBSELSIT'] = selsit
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['userresp'] = seluser
        dtmpl['selsit'] = selsit
        # dtmpl['faturas']=sql_param.get_fat_reldeb(strdataini)
        dtmpl['debitos'] = sql_param.get_deb_reldeb(strdataini, seluser)
        if dtmpl['debitos'] and selsit != 'TODOS':
            if selsit != 'EM ABERTO':
                dtmpl['debitos'] = [x for x in dtmpl['debitos'] if x['SITUACAO'] == selsit]
            else:
                dtmpl['debitos'] = [x for x in dtmpl['debitos'] if x['SITUACAO'] != 'RECEBIDO']
        dtmpl['creditos'] = sql_param.get_fat_relcre(strdataini)
        dtmpl['sel_users'] = [('TODOS', 'TODOS')] + [(x['NOME'], x['NOME']) for x in
                                                     sql_param.get_lst_users(nivel=9, depto=u'ADMINIST')]
        dtmpl['sel_situacao'] = [('TODOS', 'TODOS'), ('SEM CONTATO', 'SEM CONTATO'), ('VERIFICANDO', 'VERIFICANDO'),
                                 ('LIG.DEPOIS', 'LIG.DEPOIS'), ('RECEBIDO', 'RECEBIDO'), ('EM ACORDO', 'EM ACORDO'),
                                 ('EM ABERTO', 'EM ABERTO')]
        dtmpl['resumodeb'] = sql_param.get_deb_resumo(strdataini)
        print 'geral', dtmpl['resumodeb']
        print '\n'
        dtmpl['session'] = cherrypy.session
        resgraf = list()
        for res in dtmpl['resumodeb']:
            print res, 'user', resgraf
            encontrado = next((x for x in resgraf if x['USERRESP'] == res['USERRESP']), None)
            if not encontrado:
                vlrrecebido = decimal.Decimal(0)
                if res['SITUACAO'] == 'RECEBIDO':
                    vlrrecebido = res['TOTAL']
                res['RECEBIDO'] = vlrrecebido
                resgraf.append(res)
            else:
                encontrado['TOTAL'] += res['TOTAL']
                if res['SITUACAO'] == 'RECEBIDO':
                    encontrado['RECEBIDO'] += res['TOTAL']
        dtmpl['resgraf'] = resgraf
        dtmpl['resdialstdia'] = list(set([x['VENCMENS'] for x in dtmpl['debitos']]))
        dtmpl['resdialstuser'] = list(set([x['USERRESP'] for x in dtmpl['debitos']]))
        dtmpl['resdialstuser'].sort()
        dtmpl['resdialstdia'].sort()
        dtmpl['resdia'] = list()
        dtmpl['total_geral'] = {
            'TOTAL_ARECEBER': decimal.Decimal(0),
            'TOTAL_RECEBIDO': decimal.Decimal(0),
            'TOTAL_PAGO': decimal.Decimal(0),
            'SALDO_ARECEBER': decimal.Decimal(0),
        }
        for res in dtmpl['debitos']:
            dtmpl['total_geral']['TOTAL_ARECEBER'] += res['VLR_TOTAL']
            dtmpl['total_geral']['TOTAL_RECEBIDO'] += res['TOTAL_RECEBIDO'] + res['TOTAL_REC_PARCIAL']
            dtmpl['total_geral']['TOTAL_PAGO'] += (res['VLR_PAGO'] or decimal.Decimal(0))
            dtmpl['total_geral']['SALDO_ARECEBER'] += res['VLR_TOTAL'] - (res['VLR_PAGO'] or decimal.Decimal(0))

            tmplstuser = [x for x in dtmpl['resdia'] if
                          x['USERRESP'] == res['USERRESP'] and x['DIA'] == res['VENCMENS']]
            if len(tmplstuser) > 0:
                tmpuser = tmplstuser[0]
            else:
                tmpuser = {
                    'USERRESP': res['USERRESP'],
                    'DIA': res['VENCMENS'],
                    'RECEBER': decimal.Decimal(0),
                    'RECEBIDO': decimal.Decimal(0),
                }
                dtmpl['resdia'].append(tmpuser)
            if tmpuser:
                tmpuser['RECEBER'] += res['VLR_TOTAL']
                tmpuser['RECEBIDO'] += next(
                    (x['TOTAL_GERAL'] for x in dtmpl['creditos'] if x['NOMCLIENTE'] == res['NMCLI']),
                    decimal.Decimal(0))
        dtmpl['resgrafdia'] = list()
        for tmpuser in dtmpl['resdialstuser']:
            tmpuservalor = {
                'NMUSER': tmpuser,
                'LSTRECEBIDO': list(),
                'LSTRECEBER': list(),
            }
            dtmpl['resgrafdia'].append(tmpuservalor)
            for tmpdia in dtmpl['resdialstdia']:
                tmpitem = next((x for x in dtmpl['resdia'] if x['USERRESP'] == tmpuser and x['DIA'] == tmpdia), None)
                if tmpitem:
                    tmpuservalor['LSTRECEBIDO'].append(tmpitem['RECEBIDO'])
                    tmpuservalor['LSTRECEBER'].append(tmpitem['RECEBER'])
                else:
                    tmpuservalor['LSTRECEBIDO'].append(decimal.Decimal(0))
                    tmpuservalor['LSTRECEBER'].append(decimal.Decimal(0))
        tmpl = env.get_template('treldebitos.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @require(nivel(9))
    def treldebadd(self, **kwargs):
        selcli = 0
        ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
        strdataini = ini_mes.strftime('%d.%m.%Y')
        if kwargs.has_key('commit'):
            strdataini = kwargs.get('COMPETENCIA', '0')
            selcompt = strdataini[3:].replace('.', '/')
            if kwargs.get('commit', '0') == u'cancel':
                raise cherrypy.HTTPRedirect('/treldebitos?dt_ini=%s' % selcompt)
        if kwargs.has_key('ID'):
            selcli = kwargs.get('CODCLI', '0')
            strdataini = kwargs.get('COMPETENCIA', '0')
            selcompt = strdataini[3:].replace('.', '/')
            if sql_param.update_reldeb(kwargs):
                raise cherrypy.HTTPRedirect('/treldebitos?dt_ini=%s' % selcompt)
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['faturas'] = sql_param.get_fat_reldeb_det(strdataini, selcli)
        dtmpl['debitos'] = sql_param.get_deb_reldebdet(strdataini, selcli)
        dtmpl['session'] = cherrypy.session
        dtmpl['sel_status'] = [('S', 'S'), ('N', 'N')]
        dtmpl['sel_sit'] = [('SEM CONTATO', 'SEM CONTATO'), ('VERIFICANDO', 'VERIFICANDO'),
                            ('LIG.DEPOIS', 'LIG.DEPOIS'), ('RECEBIDO', 'RECEBIDO'), ('EM ACORDO', 'EM ACORDO')]
        dtmpl['erros'] = dict()
        tmpl = env.get_template('treldebitosdet.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @require(nivel(9))
    def treldebdet(self, **kwargs):
        if kwargs.has_key('codcli'):
            selcli = int(kwargs['codcli'])
        else:
            selcli = 0
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['faturas'] = sql_param.get_fat_reldeb_det(strdataini, selcli)
        dtmpl['debitos'] = sql_param.get_deb_reldebdet(strdataini, selcli)
        dtmpl['sel_users'] = [(x['NOME'], x['NOME']) for x in sql_param.get_lst_users(nivel=9, depto=u'ADMINIST')]
        dtmpl['session'] = cherrypy.session
        dtmpl['sel_status'] = [('S', 'S'), ('N', 'N')]
        dtmpl['sel_sit'] = [('SEM CONTATO', 'SEM CONTATO'), ('VERIFICANDO', 'VERIFICANDO'),
                            ('LIG.DEPOIS', 'LIG.DEPOIS'), ('RECEBIDO', 'RECEBIDO'), ('EM ACORDO', 'EM ACORDO')]
        dtmpl['erros'] = dict()
        tmpl = env.get_template('treldebitosdet.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @require(nivel(9))
    @require(name_is(['LEANDRO_TI', 'FATIMA']))
    def tcusto_cli(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        dtmpl = dict()
        dtmpl['sel_cli'] = self.getcombocli(selcli)
        dtmpl['scompt'] = selcompt
        dtmpl['dados_cli'] = self.getdadosfincli(selcli, datainicial)
        desemp_cli = list()
        htmlarq = u''
        if selcli > 0:
            desemp_cli.extend(self.getdesempcli(selcli, datainicial, datafinal))
            if 'FILIAIS' in dtmpl['dados_cli']:
                for dfilial in dtmpl['dados_cli']['FILIAIS']:
                    desemp_cli.extend(self.getdesempcli(dfilial['CODIGO'], datainicial, datafinal))

            dtmpl['TOTCONTABIL'] = {'CUSTO': 0.0, 'TEMPO': 0.0,
                                    'PORCMENS': dtmpl['dados_cli']['CLIPORCCONTABIL'] or 0.0, 'VLRMENS': 0.0,
                                    'MENSMIN': 0.0, 'MENSTEMPO': 0.0}
            dtmpl['TOTFISCAL'] = {'CUSTO': 0.0, 'TEMPO': 0.0, 'PORCMENS': dtmpl['dados_cli']['CLIPORCFISCAL'] or 0.0,
                                  'VLRMENS': 0.0, 'MENSMIN': 0.0, 'MENSTEMPO': 0.0}
            dtmpl['TOTDP'] = {'CUSTO': 0.0, 'TEMPO': 0.0, 'PORCMENS': dtmpl['dados_cli']['CLIPORCDP'] or 0.0,
                              'VLRMENS': 0.0, 'MENSMIN': 0.0, 'MENSTEMPO': 0.0}
            dtmpl['TOTCONTABIL']['VLRMENS'] = (dtmpl['dados_cli']['VLR_MENSALIDADE'] or decimal.Decimal(
                0.0)) * decimal.Decimal(dtmpl['TOTCONTABIL']['PORCMENS']) / decimal.Decimal(100.0)
            dtmpl['TOTFISCAL']['VLRMENS'] = (dtmpl['dados_cli']['VLR_MENSALIDADE'] or decimal.Decimal(
                0.0)) * decimal.Decimal(dtmpl['TOTFISCAL']['PORCMENS']) / decimal.Decimal(100.0)
            dtmpl['TOTDP']['VLRMENS'] = (dtmpl['dados_cli']['VLR_MENSALIDADE'] or decimal.Decimal(
                0.0)) * decimal.Decimal(dtmpl['TOTDP']['PORCMENS']) / decimal.Decimal(100.0)
            dtmpl['TOTCONTABIL']['MENSMIN'] = dtmpl['TOTCONTABIL']['VLRMENS'] / decimal.Decimal(10080.0)
            dtmpl['TOTFISCAL']['MENSMIN'] = dtmpl['TOTFISCAL']['VLRMENS'] / decimal.Decimal(10080.0)
            dtmpl['TOTDP']['MENSMIN'] = dtmpl['TOTDP']['VLRMENS'] / decimal.Decimal(10080.0)
            for duser in desemp_cli:
                if duser['ID_DEPTO'] == 4:
                    dtmpl['TOTCONTABIL']['CUSTO'] += duser['CUSTO_TOTAL']
                    dtmpl['TOTCONTABIL']['TEMPO'] += duser['TOTAL']
                if duser['ID_DEPTO'] == 3:
                    dtmpl['TOTFISCAL']['CUSTO'] += duser['CUSTO_TOTAL']
                    dtmpl['TOTFISCAL']['TEMPO'] += duser['TOTAL']
                if duser['ID_DEPTO'] == 2:
                    dtmpl['TOTDP']['CUSTO'] += duser['CUSTO_TOTAL']
                    dtmpl['TOTDP']['TEMPO'] += duser['TOTAL']
                htmlarq += u'<tr>'
                htmlarq += u'<td> %s </td>' % unicode(duser['CODCLI'])
                htmlarq += u'<td> %s </td>' % unicode(duser['NOME'])
                htmlarq += u'<td> %s </td>' % unicode(duser['NMDEPTO'])
                htmlarq += u'<td> %s </td>' % unicode(duser['TOTAL'])
                htmlarq += u'<td> %s </td>' % unicode(duser['SALARIO_MES'])
                htmlarq += u'<td> %s </td>' % unicode(duser.get('CUSTO_TOTAL', ''))
                htmlarq += u'</tr>'
            dtmpl['TOTCONTABIL']['MENSTEMPO'] = float(dtmpl['TOTCONTABIL']['MENSMIN']) * dtmpl['TOTCONTABIL']['TEMPO']
            dtmpl['TOTFISCAL']['MENSTEMPO'] = float(dtmpl['TOTFISCAL']['MENSMIN']) * dtmpl['TOTFISCAL']['TEMPO']
            dtmpl['TOTDP']['MENSTEMPO'] = float(dtmpl['TOTDP']['MENSMIN']) * dtmpl['TOTDP']['TEMPO']

        dtmpl['tbl_arq'] = htmlarq
        dtmpl['custo_cli'] = desemp_cli
        dtmpl['custo_geral'] = sum(d['CUSTO_TOTAL'] for d in desemp_cli)
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('tcustocli.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def relequipe_cli(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        dtmpl = dict()
        dtmpl['sel_cli'] = self.getcombocli(selcli)
        desemp_cli = None
        if selcli > 0:
            desemp_cli = sql_param.get_equipe_cli(selcli)
        dtmpl['tbl_arq'] = desemp_cli
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('relequipe.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def lancfin_cli(self, **kwargs):
        selcli = 0
        selano = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            selano = int(kwargs['dt_ini']) or 0
        if selano == 0:
            ini_mes = datetime.date.today()
            selano = ini_mes.year
        dtmpl = dict()
        dtmpl['sel_cli'] = self.getcombocli(selcli)
        dtmpl['scompt'] = selano
        desemp_cli = None
        dados_cli = None
        qtdnpago = 0
        if selcli > 0:
            desemp_cli = sql_param.get_finlancamentos_cli(selcli, selano)

        dtmpl['tbl_arq'] = desemp_cli
        dtmpl['dados_cli'] = dados_cli
        dtmpl['qtdnpago'] = qtdnpago
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('lancfin_cli.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def servprot_cli(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = int(kwargs['idcli']) or 0

        ini_mes = datetime.date.today().replace(day=1)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')

        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime('%d.%m.%Y')
        selano = strdataini[3:].replace('.', '/')

        dtmpl = dict()
        seltodos = u'seleted' if selcli == 10000 else ''
        addtodos = u'<option value="10000" %(sel)s >Todos</option>' % {'sel': seltodos}
        dtmpl['sel_cli'] = self.getcombocli(selcli) + addtodos

        dtmpl['scompt'] = selano
        desemp_cli = None
        dados_cli = None
        qtdnpago = 0
        if selcli > 0:
            desemp_cli = sql_param.get_servprot_cli(selcli, strdataini, strdatafim)

        dtmpl['tbl_arq'] = desemp_cli
        dtmpl['dados_cli'] = dados_cli
        dtmpl['qtdnpago'] = qtdnpago
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('servprot_cli.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    @require(nivel(9))
    def relquitacao_cli(self, **kwargs):
        selcli = 0
        selano = 0
        sfat = 'S'
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            selano = (int(kwargs['dt_ini']) or 0)
        if kwargs.has_key('sfat'):
            sfat = (kwargs['sfat'] or 'N')
        dtmpl = dict()
        dtmpl['sel_cli'] = self.getcombocli(selcli)
        dtmpl['scompt'] = selano
        dtmpl['sfat'] = sfat
        desemp_cli = None
        dados_cli = None
        qtdnpago = 0
        if selcli > 0:
            desemp_cli = sql_param.get_quitacao_cli(selcli, selano)
            dados_cli = sql_param.get_cliente_dados(selcli)
            qtdnpago = sql_param.get_qtd_nao_quitacao_cli(selcli, selano)

        dtmpl['tbl_arq'] = desemp_cli
        dtmpl['dados_cli'] = dados_cli
        dtmpl['qtdnpago'] = qtdnpago
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('relquitacao_cli.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    @require(nivel(9))
    def lstlanchist(self, **kwargs):
        selcli = 0
        sfat = 'S'
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selano = strdataini[3:].replace('.', '/')
        selcompt = strdataini[3:5]+strdataini[8:10]
        dtmpl = dict()
        dtmpl['scompt'] = selano
        lstreg = sql_param.get_finlancamentos_compt(selcompt)

        dtmpl['tbl_arq'] = lstreg
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('lstlanchist.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    @require(nivel(9))
    def totfindescr(self, **kwargs):
        dados_tmpl = dict()
        ini_mes = datetime.date.today().replace(day=1)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            ini_mes = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        strdataini = ini_mes.strftime('%Y%m%d')

        datafinal = contesputil.add_one_month(datainicial)
        datafinalparam = datafinal
        if kwargs.has_key('dt_fim'):
            strdatafim = '01.' + kwargs['dt_fim'].replace('/', '.')
            datafinalparam = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
            datafinal = datafinalparam
        strdatafim = datafinal.strftime("%Y%m%d").replace('/', '.')
        selano = ini_mes.strftime('%m/%Y')
        dados_tmpl['sleg'] = 'N'
        if kwargs.has_key('sleg'):
            dados_tmpl['sleg'] = kwargs['sleg']
        dados_tmpl['lsthist'] = sql_param.get_lst_tot_hist(datainicial, datafinal, dados_tmpl['sleg'])
        dados_tmpl['totgpag'] = sum(l['TOTAL_PAGO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'D') - sum(
            l['TOTAL_PAGO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'C')
        dados_tmpl['totgnpag'] = sum(l['TOTAL_NAO_PAGO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'D') - sum(
            l['TOTAL_NAO_PAGO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'C')
        dados_tmpl['totgeral'] = sum(l['TOTAL_GERAL'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'D') - sum(
            l['TOTAL_GERAL'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'C')
        dados_tmpl['totgcanc'] = sum(l['TOTAL_CANCELADO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'D') - sum(
            l['TOTAL_CANCELADO'] for l in dados_tmpl['lsthist'] if l['DEBCRED'] == 'C')
        dados_tmpl['scomptini'] = datainicial.strftime('%m/%Y')
        dados_tmpl['scomptfim'] = datafinalparam.strftime('%m/%Y')
        tbl_html = u''
        usuarios = None

        if dados_tmpl['sleg'] == 'S':
            cr_users = contesputil.ret_cursor('fb')
            sql = '''
            select usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
            ,usuario.dat_inicio , usuario.dat_fim
            ,usuario.depto , depto.id_depto ,usuario.CLIENTE_DOMINIO,usuario.EMPREG_DOMINIO,usuario.nivel
            ,usuario.encarregado ,usuario.funcao , 0 AS CUSTO_SALARIO , 0 as SALARIO_MES
            from usuario
            inner join depto on depto.nomedepartamento = usuario.depto
            where depto.id_depto = %(iddepto)s
             and ((usuario.dat_inicio >= '%(pcomp)s') or (usuario.dat_inicio is null))
             and ((usuario.dat_fim <= '%(pcomp)s') or (usuario.dat_fim is null))
            order by usuario.nome
             ''' % {'pcomp': datainicial.strftime('%d.%m.%Y'), 'iddepto': 16}
            cr_users.execute(sql.encode('ascii'))

            usuarios = contesputil.dictcursor3(cr_users)
            for duser in usuarios:
                sql = '''
                SELECT bethadba.foempregados.CODI_EMP,bethadba.foempregados.I_EMPREGADOS, bethadba.fobasesserv.SALARIO_MES
                FROM  bethadba.foempregados
                INNER JOIN  bethadba.fobasesserv ON bethadba.foempregados.CODI_EMP=bethadba.fobasesserv.CODI_EMP
                AND bethadba.foempregados.I_EMPREGADOS=bethadba.fobasesserv.I_EMPREGADOS
                WHERE bethadba.foempregados.CODI_EMP in ( %(codi_emp)s )
                AND bethadba.foempregados.I_EMPREGADOS = %(i_empreg)s
                AND bethadba.fobasesserv.TIPO_PROCESS=11
                AND bethadba.fobasesserv.COMPETENCIA='%(dtini)s'
                AND bethadba.fobasesserv.rateio=1
                ORDER BY bethadba.foempregados.NOME
                ''' % {'dtini': datainicial.strftime('%Y%m%d'), 'codi_emp': duser['CLIENTE_DOMINIO'],
                       'i_empreg': duser['EMPREG_DOMINIO']}
                cr = contesputil.ret_cursor('do')

                cr.execute(sql)
                dados_empreg = contesputil.dictcursor2(cr)

                if len(dados_empreg) > 0:
                    duser.update(dados_empreg[0])
                    duser['SALARIO_MES'] = float(duser['SALARIO_MES'] or 0.0)
                    salemp = duser['SALARIO_MES']
                    smpart2 = 0.0
                    smpart3 = 0.0
                    porcent_contabilidade = 0
                    if duser['CODI_EMP'] == 45:
                        porcent_contabilidade = 27.27
                    smpart2 = (salemp * porcent_contabilidade / 100.0) + (salemp / 12.0) + (
                                salemp / 12.0 + (salemp / 12.0) / 3.0)
                    smpart3 = 362.20
                    duser['CUSTO_SALARIO'] = salemp + smpart2 + smpart3

        dados_tmpl['tb_det'] = tbl_html
        dados_tmpl['leg_users'] = usuarios
        tmpl = env.get_template('totfindescr.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    @require(nivel(9))
    @require(name_is(['LEANDRO_TI', 'FATIMA']))
    def tcusto_ini(self, **kwargs):
        dados_tmpl = dict()
        ini_mes = datetime.date.today().replace(day=1)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            ini_mes = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        strdataini = ini_mes.strftime('%Y%m%d')
        selano = ini_mes.strftime('%m/%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%Y%m%d").replace('/', '.')
        if kwargs.has_key('setor'):
            numdepto = kwargs['setor']
        else:
            numdepto = 0
        tbl_html = u''
        cr_users = contesputil.ret_cursor('fb')
        sql = '''
        select grupousr.id as grp_id, grupousr.descricao as grp_nome
        ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
        ,usuario.dat_inicio , usuario.dat_fim
        ,usuario.depto , depto.id_depto ,usuario.CLIENTE_DOMINIO,usuario.EMPREG_DOMINIO,usuario.nivel
        ,usuario.encarregado ,usuario.funcao 
        from usuario
        left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id=gp.IDGRP
        left join depto on depto.nomedepartamento = usuario.depto
        where usuario.EMPREG_DOMINIO is not null
         and depto.id_depto = %(iddepto)s
        order by usuario.nome
         ''' % {'pcomp': ini_mes.strftime('%d.%m.%Y'), 'iddepto': numdepto}
        cr_users.execute(sql.encode('ascii'))

        usuarios = contesputil.dictcursor3(cr_users)
        sql = '''
        SELECT bethadba.foempregados.CODI_EMP,bethadba.foempregados.I_EMPREGADOS,bethadba.foempregados.NOME, bethadba.fobasesserv.SALARIO_MES
         FROM  bethadba.foempregados
        INNER JOIN  bethadba.fobasesserv ON bethadba.foempregados.CODI_EMP=bethadba.fobasesserv.CODI_EMP
         AND bethadba.foempregados.I_EMPREGADOS=bethadba.fobasesserv.I_EMPREGADOS
        WHERE bethadba.foempregados.CODI_EMP in (45,565,780,966 )
        AND bethadba.fobasesserv.TIPO_PROCESS=11
        AND bethadba.fobasesserv.COMPETENCIA='%(dtini)s'
        AND bethadba.fobasesserv.rateio=1
        ORDER BY bethadba.foempregados.NOME

        ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cr = contesputil.ret_cursor('do')
        cr.execute(sql)
        rowrs = cr.fetchall()
        if rowrs:
            tbl_html = u'<thead><th>Cliente</th><th>Cod.Empregado</th><th>Nome</th><th>Equipe</th><th>Depto</th><th>Salario</th><th>Impostos + provises</th><th>Beneficios</th><th>Sal.Total</th><th>Custo Minuto</th><th>Minutos Trab.</th><th>Custo Trab.</th></tr>'
        for serv in rowrs:
            perfil_user = next(
                (x for x in usuarios if (x['CLIENTE_DOMINIO'] == serv[0]) and (x['EMPREG_DOMINIO'] == serv[1])), {})
            if perfil_user:
                tbl_html += u'<tr>'
                tbl_html += u'<td> %s </td>' % serv[0]
                tbl_html += u'<td> %s </td>' % serv[1]
                tbl_html += u'<td> %s </td>' % serv[2].decode('utf-8', 'replace')
                if perfil_user:
                    tbl_html += u'<td> %s </td>' % unicode(perfil_user['GRP_NOME'] or ' ')
                    tbl_html += u'<td> %s </td>' % unicode(perfil_user['DEPTO'] or ' ')
                else:
                    tbl_html += u'<td> %s </td>' % unicode(' ')
                    tbl_html += u'<td> %s </td>' % unicode(' ')
                tbl_html += u'<td> %s </td>' % serv[3]
                salemp = 0.0
                smpart2 = 0.0
                smpart3 = 0.0
                tempo_user_qtd = 0
                tempo_user_tempo = 0
                custo_total = 0
                custo_total_min = 0
                if perfil_user:
                    min_mes = float(perfil_user['HRS_META'] or 480.0) * 21.0
                    # salminut = float(serv[3] or 0.0) / min_mes
                    salemp = float(serv[3] or 0.0)
                    porcent_contabilidade = 0
                    if serv[0] == 45:
                        porcent_contabilidade = 27.27
                    smpart2 = (salemp * porcent_contabilidade / 100.0) + (salemp / 12.0) + (
                                salemp / 12.0 + (salemp / 12.0) / 3.0)
                    smpart3 = 362.20
                    custo_total = salemp + smpart2 + smpart3
                    if min_mes > 0.0:
                        custo_total_min = custo_total / min_mes
                    else:
                        custo_total_min = custo_total / 480.0
                    crdesmp = contesputil.ret_cursor('fb')
                    sql = '''select * from desempenho_tempo_user('%(dtini)s','%(dtfim)s', %(setor)s ,'%(nome)s')
                    ''' % {'dtini': datainicial.strftime('%d.%m.%Y'), 'dtfim': datafinal.strftime('%d.%m.%Y'),
                           'setor': perfil_user['ID_DEPTO'], 'nome': perfil_user['NOME']}
                    crdesmp.execute(sql.encode('ascii'))
                    desemp_user = contesputil.dictcursor(crdesmp)
                    if desemp_user:
                        tempo_user_qtd = desemp_user[0]['QTD']
                        tempo_user_tempo = desemp_user[0]['TEMPO']

                tbl_html += u'<td> %.2f </td>' % smpart2
                tbl_html += u'<td> %.2f </td>' % smpart3
                tbl_html += u'<td> %.2f </td>' % custo_total
                tbl_html += u'<td> %.2f </td>' % custo_total_min
                tbl_html += u'<td> %.2f </td>' % tempo_user_tempo
                tbl_html += u'<td> %.2f </td>' % (custo_total_min * tempo_user_tempo)
                tbl_html += u'</tr>'
        dados_tmpl['scompt'] = selano
        dados_tmpl['setor'] = numdepto
        if numdepto == '2':
            dados_tmpl['seldp'] = 'selected'
        if numdepto == '3':
            dados_tmpl['selfs'] = 'selected'
        if numdepto == '4':
            dados_tmpl['selct'] = 'selected'

        dados_tmpl['tb_det'] = tbl_html
        tmpl = env.get_template('custo_ini.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    def lista_users_folha(self, compt):
        datainicial = compt
        strdataini = compt.strftime('%Y%m%d')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%Y%m%d")
        sql = '''
        SELECT bethadba.foempregados.CODI_EMP,bethadba.foempregados.I_EMPREGADOS,bethadba.foempregados.NOME, bethadba.fobasesserv.SALARIO_MES
         FROM  bethadba.foempregados
        INNER JOIN  bethadba.fobasesserv ON bethadba.foempregados.CODI_EMP=bethadba.fobasesserv.CODI_EMP
         AND bethadba.foempregados.I_EMPREGADOS=bethadba.fobasesserv.I_EMPREGADOS
        WHERE bethadba.foempregados.CODI_EMP in (45,565,780,966 )
        AND bethadba.fobasesserv.TIPO_PROCESS=11
        AND bethadba.fobasesserv.COMPETENCIA='%(dtini)s'
        AND bethadba.fobasesserv.rateio=1
        ORDER BY bethadba.foempregados.NOME
        ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cr = contesputil.ret_cursor('do')
        cr.execute(sql)
        emprg_dominio = contesputil.dictcursor3(cr)
        for emprg in emprg_dominio:
            emprg['SALEMP'] = emprg['SALARIO_MES']
            emprg['SMPART2'] = 0.0
            emprg['SMPART3'] = 0.0
            emprg['TEMPO_USER_QTD'] = 0
            emprg['TEMPO_USER_TEMPO'] = 0
            emprg['CUSTO_TOTAL'] = 0
            emprg['CUSTO_TOTAL_MIN'] = 0
            emprg['QTD'] = 0
            emprg['TEMPO'] = 0
            cr_users = contesputil.ret_cursor('fb')
            sql = '''
            select first 1 grupousr.id as grp_id, grupousr.descricao as grp_nome
            ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
            ,usuario.dat_inicio , usuario.dat_fim
            ,usuario.depto , depto.id_depto ,usuario.CLIENTE_DOMINIO,usuario.EMPREG_DOMINIO,usuario.nivel
            ,usuario.encarregado ,usuario.funcao 
            from usuario
            left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
            left join grupousr on grupousr.id=gp.IDGRP
            left join depto on depto.nomedepartamento = usuario.depto
            where usuario.EMPREG_DOMINIO = %(idemprg)s
             and usuario.CLIENTE_DOMINIO = %(codi_emp)s
            order by usuario.nome
             ''' % {'pcomp': datainicial.strftime('%d.%m.%Y'), 'idemprg': emprg['I_EMPREGADOS'],
                    'codi_emp': emprg['CODI_EMP']}
            cr_users.execute(sql.encode('ascii'))
            usuarios = contesputil.dictcursor3(cr_users)
            for us in usuarios:
                emprg['GRP_ID'] = us['GRP_ID']
                emprg['GRP_NOME'] = us['GRP_NOME']
                emprg['IDUSER'] = us['IDUSER']
                emprg['LOGIN'] = us['NOME']
                emprg['HRS_META'] = us['HRS_META'] or 0.0
                emprg['DAT_INICIO'] = us['DAT_INICIO']
                emprg['DAT_FIM'] = us['DAT_FIM']
                emprg['ID_DEPTO'] = us['ID_DEPTO']
                emprg['DEPTO'] = us['DEPTO']
                emprg['CLIENTE_DOMINIO'] = us['CLIENTE_DOMINIO']
                emprg['EMPREG_DOMINIO'] = us['EMPREG_DOMINIO']
                emprg['NIVEL'] = us['NIVEL']
                emprg['ENCARREGADO'] = us['ENCARREGADO']
                emprg['FUNCAO'] = us['FUNCAO']
                emprg['MIN_MES'] = float(emprg.get('HRS_META', 0.0)) * 21.0
                porcent_contabilidade = decimal.Decimal(0.0)
                if emprg['CLIENTE_DOMINIO'] == 45:
                    porcent_contabilidade = decimal.Decimal(27.27)
                emprg['SMPART2'] = (emprg['SALEMP'] * porcent_contabilidade / decimal.Decimal(100.0)) + (
                        emprg['SALEMP'] / decimal.Decimal(12.0)) + (emprg['SALEMP'] / decimal.Decimal(12.0) + (
                            emprg['SALEMP'] / decimal.Decimal(12.0)) / decimal.Decimal(3.0))
                emprg['SMPART3'] = decimal.Decimal(362.20)
                emprg['CUSTO_TOTAL'] = float(emprg['SALEMP'] + emprg['SMPART2'] + emprg['SMPART3'])
                if emprg['MIN_MES'] > 0.0:
                    emprg['CUSTO_TOTAL_MIN'] = float(emprg['CUSTO_TOTAL']) / emprg['MIN_MES']

                crdesmp = contesputil.ret_cursor('fb')
                sql = '''select * from desempenho_tempo_user('%(dtini)s','%(dtfim)s', %(setor)s ,'%(nome)s') 
                ''' % {'dtini': datainicial.strftime('%d.%m.%Y'), 'dtfim': datafinal.strftime('%d.%m.%Y'),
                       'setor': emprg['ID_DEPTO'], 'nome': emprg['LOGIN']}
                crdesmp.execute(sql.encode('ascii'))
                desemp_user = contesputil.dictcursor(crdesmp)
                if desemp_user:
                    emprg['QTD'] = desemp_user[0]['QTD']
                    emprg['TEMPO'] = desemp_user[0]['TEMPO']

            emprg['MIN_MES'] = float(emprg.get('HRS_META', 480.0)) * 21.0
            porcent_contabilidade = decimal.Decimal(0.0)
            if emprg.get('CLIENTE_DOMINIO', 0) == 45:
                porcent_contabilidade = decimal.Decimal(27.27)
            emprg['SMPART2'] = (emprg['SALEMP'] * porcent_contabilidade / decimal.Decimal(100.0)) + (
                        emprg['SALEMP'] / decimal.Decimal(12.0)) + (emprg['SALEMP'] / decimal.Decimal(12.0) + (
                        emprg['SALEMP'] / decimal.Decimal(12.0)) / decimal.Decimal(3.0))
            emprg['SMPART3'] = decimal.Decimal(362.20)
            emprg['CUSTO_TOTAL'] = float(emprg['SALEMP'] + emprg['SMPART2'] + emprg['SMPART3'])
            if emprg['MIN_MES'] > 0.0:
                emprg['CUSTO_TOTAL_MIN'] = float(emprg['CUSTO_TOTAL']) / emprg['MIN_MES']
            else:
                emprg['CUSTO_TOTAL_MIN'] = float(emprg['CUSTO_TOTAL']) / float(480.0 * 21.0)
        return emprg_dominio

    @cherrypy.expose
    @require(nivel(9))
    def lista_users_emprg(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            ini_mes = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        compt = ini_mes
        datainicial = ini_mes
        strdataini = compt.strftime('%Y%m%d')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%Y%m%d")
        sql = '''
        SELECT bethadba.foempregados.CODI_EMP,bethadba.foempregados.I_EMPREGADOS,bethadba.foempregados.NOME
         FROM  bethadba.foempregados
        WHERE bethadba.foempregados.CODI_EMP in (45,565,780,966 )
        ORDER BY bethadba.foempregados.NOME
        ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cr = contesputil.ret_cursor('do')
        cr.execute(sql)
        emprg_dominio = contesputil.dictcursor3(cr)
        html = u'<table border=1>'
        for emprg in emprg_dominio:

            html += '<tr>'
            for dados in emprg:
                html += '<td>' + unicode(dados) + '=>' + unicode(emprg[dados]) + '</td>'
            cr_users = contesputil.ret_cursor('fb')
            sql = '''
            select first 1 grupousr.id as grp_id, grupousr.descricao as grp_nome
            ,usuario.iduser, usuario.nome , usuario.nomcompleto , usuario.hrs_meta , usuario.login_dominio
            ,usuario.dat_inicio , usuario.dat_fim
            ,usuario.depto , depto.id_depto ,usuario.CLIENTE_DOMINIO,usuario.EMPREG_DOMINIO,usuario.nivel
            ,usuario.encarregado ,usuario.funcao 
            from usuario
            left join sp_grupousr_participante_vig(usuario.iduser,'%(pcomp)s') gp on gp.IDUSR=usuario.iduser
            left join grupousr on grupousr.id=gp.IDGRP
            left join depto on depto.nomedepartamento = usuario.depto
            where usuario.EMPREG_DOMINIO = %(idemprg)s
             and usuario.CLIENTE_DOMINIO = %(codi_emp)s
            order by usuario.nome
             ''' % {'pcomp': datainicial.strftime('%d.%m.%Y'), 'idemprg': emprg['I_EMPREGADOS'],
                    'codi_emp': emprg['CODI_EMP']}
            cr_users.execute(sql.encode('ascii'))
            usuarios = contesputil.dictcursor3(cr_users)
            for us in usuarios:
                print us['NOME']
                html += '<td> IDUSER =>' + unicode(us['IDUSER']) + '</td>'
                html += '<td> LOGIN CONTROLE =>' + unicode(us['NOME']) + '</td>'
            html += '</tr>'
        html += '</ table>'
        return html

    def lista_tot_depto(self, lstuser):
        totdepto = {'CONTABIL': 0.0, 'CONTABIL_SUP': 0.0,
                    'FISCAL': 0.0, 'FISCAL_SUP': 0.0, 'PESSOAL': 0.0, 'PESSOAL_SUP': 0.0,
                    'ADM': 0.0, 'LEG': 0.0, 'OUTROS': 0.0, 'GERAL': 0.0,
                    'CONTQTD': 0, 'CONTSUPQTD': 0, 'FISCQTD': 0, 'FISCSUPQTD': 0,
                    'PESQTD': 0, 'PESSUPQTD': 0, 'ADMQTD': 0, 'LEGQTD': 0, 'OUTQTD': 0,
                    'GERALQTD': 0}
        for duser in lstuser:
            id_depto_user = duser.get('ID_DEPTO', 0)
            totdepto['GERAL'] += float(duser.get('CUSTO_TOTAL', 0.0))
            totdepto['GERALQTD'] += 1
            if id_depto_user in [1, 15, 11, 14, 25, 12]:
                totdepto['ADM'] += float(duser.get('CUSTO_TOTAL', 0.0))
                totdepto['ADMQTD'] += 1
            elif id_depto_user == 16:
                totdepto['LEG'] += float(duser.get('CUSTO_TOTAL', 0.0))
                totdepto['LEGQTD'] += 1
            elif id_depto_user == 2:
                if duser.get('HRS_META', 0.0) <> 0.0:
                    totdepto['PESSOAL'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['PESQTD'] += 1
                else:
                    totdepto['PESSOAL_SUP'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['PESSUPQTD'] += 1
            elif id_depto_user == 3:
                if duser.get('HRS_META', 0.0) <> 0.0:
                    totdepto['FISCAL'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['FISCQTD'] += 1
                else:
                    totdepto['FISCAL_SUP'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['FISCSUPQTD'] += 1
            elif id_depto_user == 4:
                if duser.get('HRS_META', 0.0) <> 0.0:
                    totdepto['CONTABIL'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['CONTQTD'] += 1
                else:
                    totdepto['CONTABIL_SUP'] += float(duser.get('CUSTO_TOTAL', 0.0))
                    totdepto['CONTSUPQTD'] += 1
            else:
                totdepto['OUTROS'] += float(duser.get('CUSTO_TOTAL', 0.0))
                totdepto['OUTQTD'] += 1
        return totdepto

    @cherrypy.expose
    @require(nivel(9))
    @require(member_of('INFORMÁTICA,ADMINISTRAÇÃO'))
    @require(name_is(['LEANDRO_TI', 'FATIMA']))
    def tcusto_folha(self, **kwargs):
        dados_tmpl = dict()
        ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            ini_mes = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        strdataini = ini_mes.strftime('%Y%m%d')
        selano = ini_mes.strftime('%m/%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%Y%m%d").replace('/', '.')
        emprgd = self.lista_users_folha(ini_mes)
        dados_tmpl['scompt'] = selano
        dados_tmpl['users'] = emprgd
        dados_tmpl['totdepto'] = self.lista_tot_depto(emprgd)
        dados_tmpl['clifin'] = self.gettotfincli(datainicial)
        tmpl = env.get_template('custo_folha.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def acomp_dipj_ent(self, **kwargs):
        sdipje = u'''[
            (1,'Insumos',[ 1101, 1111, 1116, 1120, 1122, 1151, 1401, 1408, 2101, 2111, 2116, 2120, 2122, 2151, 2401, 2408 ]),
            (2,'Mercadoria',[1102, 1113, 1117, 1118, 1121, 1152, 1403, 1409, 2102, 2113, 2117, 2118, 2121, 2152, 2403, 2409 ]),
            (3,'Industrializaçao',[1124, 1125, 2124, 2125 ]),
            (4,'Devoluçao',[1201, 1202, 1203, 1204, 1208, 1209, 1410, 1411, 1918, 1919, 2201, 2202, 2203, 2204, 2208, 2209, 2410, 2411, 2918, 2919]),
            (5,'Outras',[1126, 1154, 1207, 1252, 1256, 1302, 1352, 1356, 1406, 1407, 1414, 1415, 1452, 1501, 1503, 1504, 1505, 1506, 1551, 1552, 1553, 1554, 1555, 1556, 1557, 1601, 1602, 1603, 1653, 1658, 1660, 1662, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 1914, 1915, 1916, 1917, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1934, 1949, 2126, 2154, 2252, 2256, 2302, 2352, 2356, 2406, 2407, 2414, 2415, 2501, 2503, 2504, 2505, 2506, 2551, 2552, 2553, 2554, 2555, 2556, 2557, 2651, 2653, 2658, 2660, 2662, 2901, 2902, 2903, 2904, 2905, 2906, 2907, 2908, 2909, 2910, 2911, 2912, 2913, 2914, 2915, 2916, 2917, 2920, 2921, 2922, 2923, 2924, 2925, 2934, 2949]),
            (7,'Insumos',[3101, 3127]),
            (8,'Mercadorias',[3102]),
            (9,'Devoluçao',[3201, 3202, 3211, 3503, 3553]),
            (10,'Outras',[3126, 3352, 3356, 3551, 3556, 3651, 3653, 3930, 3949]),
            ]
            '''.strip('\t')
        sql_ent = '''SELECT   coalesce(sum(bethadba.efimpent.vcon_ien),0)  as vlr_contabil, 
                    coalesce(sum(bethadba.efimpent.bcal_ien),0) as base_calc, 
                    coalesce(sum(bethadba.efimpent.vlor_ien),0) as vlr_ipi, 
                    coalesce(sum(bethadba.efimpent.vise_ien),0) as vlr_isento, 
                    coalesce(sum(bethadba.efimpent.vout_ien),0) as vlr_outros
                         FROM bethadba.efentradas, bethadba.efimpent 
                    WHERE ( bethadba.efentradas.codi_emp = %(selcli)d ) AND 
                              ( bethadba.efentradas.dent_ent >= '%(dtini)s' ) AND 
                              ( bethadba.efentradas.dent_ent < '%(dtfim)s' ) AND 
                              ( bethadba.efentradas.codi_nat in ( %(lista_cfop)s ) ) and
                             ( bethadba.efentradas.codi_emp = bethadba.efimpent.codi_emp ) AND 
                            ( bethadba.efentradas.codi_ent =  bethadba.efimpent.codi_ent ) and (bethadba.efimpent.codi_imp=2) 
            
            '''
        sdipjs = u'''[
            (1 ,'Produo',[5101, 5103, 5105, 5109, 5111, 5113, 5116, 5118, 5122, 5151, 5155, 5401, 5402, 5408, 5501, 5904, 6101, 6103, 6105, 6107, 6109, 6111, 6113, 6116, 6118, 6122, 6151, 6155, 6401, 6402, 6408, 6501, 6904 ]),
            (2 ,'Mercadoria',[5102, 5104, 5106, 5110, 5112, 5114, 5115, 5117, 5119, 5120, 5123, 5152, 5156, 5403, 5405, 5409, 5502, 5667, 6102, 6104, 6106, 6108, 6110, 6114, 6115, 6117, 6119, 6120, 6123, 6152, 6156, 6403, 6404, 6409, 6502, 6667 ]),
            (3 ,'Industrializao',[5124, 5125, 6124, 6125 ]),
            (4 ,'Devoluo',[5201, 5202, 5206, 5207, 5208, 5209, 5210, 5410, 5411, 5412, 5413, 5503, 5553, 5555, 5556, 5918, 5919, 6201, 6202, 6206, 6208, 6209, 6210, 6410, 6411, 6412, 6413, 6503, 6553, 6555, 5923, 5934, 6556, 6918,  6919, 6923, 6934 ]),
            (5 ,'Outras',[5205, 5252, 5256, 5352, 5356, 5414, 5415, 5451, 5504, 5505, 5551, 5552, 5554, 5557, 5651, 5652, 5653, 5654, 5656, 5658, 5659, 5660, 5662, 5901, 5902, 5903, 5905, 5906, 5907, 5908, 5909, 5910, 5911, 5912, 5913, 5914, 5915, 5916, 5917, 5920, 5921, 5922, 5923, 5924, 5925, 5926, 5927, 5928, 5929, 5949, 6112, 6153, 6205, 6207, 6252, 6256, 6352, 6356, 6359, 6414, 6415, 6504, 6505, 6551, 6552, 6554, 6557, 6651, 6652, 6653, 6654, 6656, 6658, 6659, 6660, 6661, 6662, 6901, 6902, 6903, 6905, 6906, 6907, 6908, 6909, 6910, 6911, 6912, 6913, 6914, 6915, 6916, 6917, 6920, 6921, 6922, 6923, 6924, 6925, 6949 ]),
            (7 ,'Produo',[7101, 7105, 7127, 7501 ]),
            (8 ,'Mercadorias',[7102, 7106 ]),
            (9 ,'Devoluo',[7201, 7202, 7210, 7211, 7553, 7556, 7930 ]),
            (10,'Outras',[7206, 7207, 7551, 7651, 7667 , 7949 ]),
            ]
            '''.strip('\t')
        sql_sai = '''SELECT   coalesce(sum(bethadba.efimpsai.vcon_isa),0)  as vlr_contabil, 
                    coalesce(sum(bethadba.efimpsai.bcal_isa),0) as base_calc, 
                    coalesce(sum(bethadba.efimpsai.vlor_isa),0) as vlr_ipi, 
                    coalesce(sum(bethadba.efimpsai.vise_isa),0) as vlr_isento, 
                    coalesce(sum(bethadba.efimpsai.vout_isa),0) as vlr_outros
                         FROM bethadba.efsaidas, bethadba.efimpsai 
                    WHERE ( bethadba.efsaidas.codi_emp = %(selcli)d ) AND 
                              ( bethadba.efsaidas.dsai_sai >= '%(dtini)s' ) AND 
                              ( bethadba.efsaidas.dsai_sai < '%(dtfim)s' ) AND 
                              ( bethadba.efsaidas.codi_nat in ( %(lista_cfop)s ) ) and
                             ( bethadba.efsaidas.codi_emp = bethadba.efimpsai.codi_emp ) AND 
                            ( bethadba.efsaidas.codi_sai =  bethadba.efimpsai.codi_sai ) and (bethadba.efimpsai.codi_imp=2) 
            '''
        tprel = 'E'
        if kwargs.has_key('tprel'):
            tprel = ((kwargs['tprel']) or 'E')
        if tprel == 'E':
            dipj_cats = eval(sdipje)
            sql_dipj = sql_ent
        else:
            dipj_cats = eval(sdipjs)
            sql_dipj = sql_sai
        dados_tmpl = dict()
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = kwargs['dt_ini'] + '0101'
        else:
            ini_mes = datetime.date.today().replace(day=1, month=1)
            strdataini = ini_mes.strftime('%Y%m%d')
        datainicial = datetime.datetime.strptime(strdataini, '%Y%m%d')
        selano = datainicial.year
        datafinal = datainicial.replace(year=selano + 1)
        strdatafim = datafinal.strftime("%Y%m%d").replace('/', '.')
        tbl_html = u''
        cursorfb = contesputil.ret_cursor('do')
        sql_cli = '''SELECT codi_emp,nome_emp FROM bethadba.geempre  where stat_emp='A' and codi_emp < 5000 order by nome_emp '''
        cursorfb.execute(sql_cli.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = ''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = 'selected'
            else:
                selec = ''
            htmlcli += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                               'nome': cli_atual[1].decode('latin-1',
                                                                                                           'replace')}
        dados_tmpl['sel_cli'] = htmlcli
        dados_tmpl['scompt'] = selano
        # Pega os dados da dipj
        if selcli > 0:
            dtot1 = {'totbas': 0, 'totout': 0, 'totipi': 0, 'totvlrcont': 0}
            dtot2 = {'totbas': 0, 'totout': 0, 'totipi': 0, 'totvlrcont': 0}
            dtot3 = {'totbas': 0, 'totout': 0, 'totipi': 0, 'totvlrcont': 0}
            tbl_html = u'<thead><th>Linha</th><th  style="width:20%" >CFOP</th><th>COM CREDITO</th><th>SEM CREDITO</th><th>IPI CREDITADO</th><th>VLR CONTABIL</th></tr>'
            for categorias in filter((lambda x: x[0] < 7), dipj_cats):
                slist_cfop = str(categorias[2]).strip('[]')
                sql = sql_dipj % {'dtini': strdataini, 'dtfim': strdatafim, 'selcli': selcli, 'lista_cfop': slist_cfop}
                cr = contesputil.ret_cursor('do')
                cr.execute(sql.encode('ascii'))
                rowrs = cr.fetchall()
                for serv in rowrs:
                    tbl_html += u'<tr>'
                    tbl_html += u'<td> %s </td>' % unicode(
                        unicode(categorias[0]) + ' - ' + unicode(categorias[1].decode('utf-8')))
                    tbl_html += u'<td> %s </td>' % unicode(slist_cfop)
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[1]
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % (serv[3] + serv[4])
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[2]
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[0]
                    tbl_html += u'</tr>'
                    dtot1['totbas'] += serv[1]
                    dtot1['totout'] += serv[3] + serv[4]
                    dtot1['totipi'] += serv[2]
                    dtot1['totvlrcont'] += serv[0]

            tbl_html += u'<tr><th><strong>Total 1</strong></th><th>&nbsp;</th><th style="text-align:right;" >%(totbas).2f</th><th style="text-align:right;" >%(totout).2f</th><th style="text-align:right;" >%(totipi).2f</th><th style="text-align:right;" >%(totvlrcont).2f</th></tr>' % dtot1
            for categorias in filter((lambda x: x[0] >= 7), dipj_cats):
                slist_cfop = str(categorias[2]).strip('[]')
                sql = sql_dipj % {'dtini': strdataini, 'dtfim': strdatafim, 'selcli': selcli, 'lista_cfop': slist_cfop}
                cr = contesputil.ret_cursor('do')
                cr.execute(sql.encode('ascii'))
                rowrs = cr.fetchall()
                for serv in rowrs:
                    tbl_html += u'<tr>'
                    tbl_html += u'<td> %s </td>' % unicode(
                        unicode(categorias[0]) + ' - ' + unicode(categorias[1].decode('utf-8')))
                    tbl_html += u'<td> %s </td>' % unicode(slist_cfop)
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[1]
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % (serv[3] + serv[4])
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[2]
                    tbl_html += u'<td style="text-align:right;" > %.2f </td>' % serv[0]
                    tbl_html += u'</tr>'
                    dtot2['totbas'] += serv[1]
                    dtot2['totout'] += serv[3] + serv[4]
                    dtot2['totipi'] += serv[2]
                    dtot2['totvlrcont'] += serv[0]
            tbl_html += u'<tr><th><strong>Total 2</strong></th><th>&nbsp;</th><th style="text-align:right;" >%(totbas).2f</th><th style="text-align:right;" >%(totout).2f</th><th style="text-align:right;" >%(totipi).2f</th><th style="text-align:right;" >%(totvlrcont).2f</th></tr>' % dtot2
            dtot3['totbas'] += dtot1['totbas'] + dtot2['totbas']
            dtot3['totout'] += dtot1['totout'] + dtot2['totout']
            dtot3['totipi'] += dtot1['totipi'] + dtot2['totipi']
            dtot3['totvlrcont'] += dtot1['totvlrcont'] + dtot2['totvlrcont']
            tbl_html += u'<tr><th><strong>Total 3</strong></th><th>&nbsp;</th><th style="text-align:right;" >%(totbas).2f</th><th style="text-align:right;" >%(totout).2f</th><th style="text-align:right;" >%(totipi).2f</th><th style="text-align:right;" >%(totvlrcont).2f</th></tr>' % dtot3

        dados_tmpl['scompt'] = selano

        dados_tmpl['tb_det'] = tbl_html
        dados_tmpl['tprel'] = tprel
        tmpl = env.get_template('acomp_dipj.html')
        dados_tmpl['session'] = cherrypy.session
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def total_viculos_dirf(self, **kwargs):
        dados_tmpl = dict()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1, month=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        selano = datainicial.year
        datafinal = datainicial.replace(year=selano + 1)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        if kwargs.has_key('setor'):
            numdepto = kwargs['setor']
        else:
            numdepto = 0
        tbl_html = u''
        sql = '''select tpserv.nome,cliente.razao
            ,servicos2.competencia
            ,sum(servicos2.qtd_item) as qtd
            from servicos2
        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
        inner join cliente on cliente.codigo=protocolo2.codclirep
        left join depto on depto.nomedepartamento=protocolo2.deptouser
        left join tpserv on tpserv.idtpserv=servicos2.idservico
        where protocolo2.emissao >= '%(dtini)s'
         and protocolo2.emissao < '%(dtfim)s'
         and protocolo2.status in ('B','N')
         and servicos2.idservico in (109,488)
        group by tpserv.nome,cliente.razao
            ,servicos2.competencia
        order by tpserv.nome,cliente.razao
            ,servicos2.competencia
        ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cr = contesputil.ret_cursor('fb')

        cr.execute(sql)
        rowrs = cr.fetchall()
        if rowrs:
            tbl_html = u'<thead><th>Cliente</th><th>Comp.</th><th>Qtd</th></tr>'
        for serv in rowrs:
            tbl_html += u'<tr>'
            tbl_html += u'<td> %s </td>' % serv[1]
            tbl_html += u'<td> %s </td>' % serv[2]
            tbl_html += u'<td> %s </td>' % serv[3]
            tbl_html += u'</tr>'
        dados_tmpl['scompt'] = selano

        dados_tmpl['tb_det'] = tbl_html
        dados_tmpl['setor'] = numdepto
        if numdepto == '2':
            dados_tmpl['seldp'] = 'selected'
        if numdepto == '3':
            dados_tmpl['selfs'] = 'selected'
        if numdepto == '4':
            dados_tmpl['selct'] = 'selected'
        dados_tmpl['session'] = cherrypy.session
        tmpl = env.get_template('total_viculos_dirf.html')
        return tmpl.render(dados_tmpl)

    @cherrypy.expose
    def detprot(self, **kwargs):
        tmpl = env.get_template('detprot2.html')
        dtmpl = dict()
        idprot = 0
        if kwargs.has_key('id'):
            idprot = (int(kwargs['id']) or 0)
        dtmpl['idprot'] = idprot
        if idprot > 0:
            # procolo
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select * from protocolo2
            where idprot2 = %(id)d
            ''' % {'id': idprot})
            protocolo = contesputil.dictcursor3(cr)
            dtmpl['protocolo'] = protocolo[0]

            # Servicos
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select servicos2.*, tpserv.nome as servnome from servicos2
            left outer join tpserv on (tpserv.idtpserv=servicos2.idservico)
                    and (tpserv.prot='S')
            where idprot2 = %(id)d
            order by SERVICOS2.IDPROTSERV
            ''' % {'id': idprot})
            servicos = contesputil.dictcursor3(cr)
            dtmpl['servicos'] = servicos
            for bscarq in dtmpl['servicos']:
                cr.execute('''Select iddeposito,nmarquivo,lido_cliente,OCTET_LENGTH(deposito.conteudo)/1024 as tam
                 from deposito where IDPROTOXOLO=%(idp)s and IDSERVPROT=%(idserv2)s
                ''' % {'idp': idprot, 'idserv2': bscarq['IDPROTSERV']})
                arqvs = contesputil.dictcursor3(cr)
                print arqvs
                bscarq['DOWNARQVS'] = arqvs

        dtmpl['session'] = cherrypy.session
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def detprocleg(self, **kwargs):
        tmpl = env.get_template('detlegproc.html')
        dtmpl = dict()
        idproc = 0
        if kwargs.has_key('id'):
            idproc = (int(kwargs['id']) or 0)
        dtmpl['idlegproc'] = idproc
        if idproc > 0:
            # procolo
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select LEG_PROCESSOS.*, CLIENTE.RAZAO, TPSERV.NOME AS NM_TPSERV, usuario.nome as NM_USERCAD  from LEG_PROCESSOS
                    left join CLIENTE on CLIENTE.CODIGO=LEG_PROCESSOS.CODCLI
                    LEFT JOIN TPSERV ON TPSERV.IDTPSERV=LEG_PROCESSOS.IDSERV
                    left join usuario on usuario.IDUSER=LEG_PROCESSOS.USERCAD
                    WHERE LEG_PROCESSOS.ID= %(id)d
            ''' % {'id': idproc})
            legprocesso = contesputil.dictcursor3(cr)
            dtmpl['processo'] = legprocesso[0]

            # servicos
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select LEG_TPSERV.*, LEG_TPLEGPROCESSO.DESCRICAO AS DESCR_SERV , usuario.nome as NM_USERRESP from LEG_TPSERV
                left join  LEG_TPLEGPROCESSO on  LEG_TPLEGPROCESSO.id=LEG_TPSERV.ID_TPSERV
                left join usuario on usuario.IDUSER=LEG_TPSERV.IDUSERRESP
                where ID_LEGPROCESSO = %(id)d
                order by DT_ENTRADA
            ''' % {'id': idproc})
            servicos = contesputil.dictcursor3(cr)
            dtmpl['servicos'] = servicos
        dtmpl['session'] = cherrypy.session
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def detprotserv(self, **kwargs):
        tmpl = env.get_template('detuser.html')
        dtmpl = dict()
        seluser = 0
        idserv = 0
        dados_user = dict()
        if kwargs.has_key('iduser'):
            seluser = (int(kwargs['iduser']) or 0)
        if kwargs.has_key('iserv'):
            idserv = (int(kwargs['iserv']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        ushtml = u''
        us = self.get_user_meta_dados(iduser=int(seluser), dtini=datainicial)
        dados_user = us[0]
        dtmpl['diasmeta'] = ((us[0]['TMP_META'] or 0) / 60)
        if dados_user:
            # Protocolos
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select * from desempenho_seluser_det
            ('%(dtini)s','%(dtfim)s',%(id_depto)s,'%(nome)s',%(idserv)s)
            ''' % {'nome': dados_user['NOME'], 'id_depto': dados_user['ID_DEPTO'], 'dtini': strdataini,
                   'dtfim': strdatafim, 'idserv': idserv})
            protocolos = contesputil.dictcursor2(cr)
            if protocolos:
                html = u'''<thead><tr><th>Processo</th><th>Servi&ccedil;o</th><th>Lan.Protocolo</th><th>Cliente</th><th>Usu&aacute;rio</th><th>Auxiliar</th><th>Qtd</th><th>Tempo Usu&aacute;rio(Min)</th><th>Tempo Auxiliar(Min)</th><th>Atraso</th></tr></thead><tbody>'''
                tqtd = 0
                ttempo = 0
                ttempo_aux = 0
                strtempo = ''
                for lanc_prot in protocolos:
                    html += u'<tr>'
                    html += u'<td> %s </td>' % unicode(lanc_prot['PROCESSO'])
                    dadoslink = {'nmserv': unicode(lanc_prot['SERV_NOME']), 'idserv': str(lanc_prot['ID_SERV']),
                                 'iduser': dados_user['IDUSER'], 'compt': kwargs['dt_ini']}
                    html += u'<td><a href="detprotserv?iduser=%(iduser)s&dt_ini=%(compt)s&iserv=%(idserv)s"> %(nmserv)s </a>  </td>' % dadoslink
                    if unicode(lanc_prot['PROCESSO']) == u'PROC.LEG.':
                        html += u'<td><a href="detprocleg?id=%(idprot)s"> %(idprot)s </a> </td>' % {
                            'idprot': unicode(lanc_prot['IDPROT'])}
                    else:
                        html += u'<td><a href="detprot?id=%(idprot)s"> %(idprot)s </a> </td>' % {
                            'idprot': unicode(lanc_prot['IDPROT'])}
                    html += u'<td> %s </td>' % unicode(lanc_prot['RAZAO'])
                    html += u'<td> %s </td>' % unicode(lanc_prot['USERRESP'])
                    html += u'<td> %s </td>' % unicode(lanc_prot['USERAUX'])
                    html += u'<td> %s </td>' % unicode(lanc_prot['QTD'])
                    html += u'<td> %s </td>' % (unicode(lanc_prot['TEMPO']) + ' (' + unicode(
                        int((lanc_prot['TEMPO'] or 0) / 60)) + ' hrs)')
                    html += u'<td> %s </td>' % (unicode(lanc_prot['TEMPO_AUX']) + ' (' + unicode(
                        int((lanc_prot['TEMPO_AUX'] or 0) / 60)) + ' hrs)')
                    atraso_txt = ''
                    if lanc_prot['ATRASO'] == u'S':
                        if lanc_prot['RESP_ATRASO'] == u'C':
                            atraso_txt = ' (CLIENTE) '
                        if lanc_prot['RESP_ATRASO'] == u'F':
                            atraso_txt = ' (CONTESP) '
                    html += u'<td> %s </td>' % (unicode(lanc_prot['ATRASO']) + atraso_txt)
                    html += u'</tr>'
                    tqtd += (lanc_prot['QTD'] or 0)
                    ttempo += (lanc_prot['TEMPO'] or 0)
                    ttempo_aux += lanc_prot['TEMPO_AUX']
                    strtempo = unicode(ttempo) + ' (' + unicode(int(ttempo / 60)) + ' hrs)'
                html += u'''</tbody><tfoot><tr><td colspan="6">TOTAL</td><td>%(qtd)s</td><td>%(tempo)s</td></tr></tfoot>''' % {
                    'qtd': tqtd, 'tempo': strtempo}
                dtmpl['tb_det'] = html
        dtmpl['seluser'] = ushtml
        dtmpl['scompt'] = selcompt
        dtmpl['dados_user'] = dados_user
        dtmpl['session'] = cherrypy.session
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def detuser(self, **kwargs):
        tmpl = env.get_template('detuser.html')
        dtmpl = dict()
        seluser = 0
        dados_user = dict()
        if kwargs.has_key('iduser'):
            seluser = (int(kwargs['iduser']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        usuarios = self.lista_user()
        ushtml = u''
        for us in usuarios:
            selec = ''
            if seluser == us['IDUSER']:
                selec = 'selected'
            ushtml += '<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': us['IDUSER'], 'sel': selec,
                                                                              'nome': us['NOME']}
        us = self.get_user_meta_dados(iduser=int(seluser), dtini=datainicial)
        dados_user = us[0]
        try:
            dtmpl['diasmeta'] = ((us[0]['TMP_META'] or 0) / 60)
        except:
            dtmpl['diasmeta'] = 0
        if dados_user:
            # Protocolos Cadastrados
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select PROCESSO, serv_nome, qtd , tempo,ID_SERV,QTD_ATRASOS from desempenho_seluser('%(dtini)s','%(dtfim)s',%(id_depto)s,'%(nome)s')
            ''' % {'nome': dados_user['NOME'], 'id_depto': dados_user['ID_DEPTO'], 'dtini': strdataini,
                   'dtfim': strdatafim})
            rowrs = cr.fetchall()
            if rowrs:
                html = u'''<thead><tr><th>Processo</th><th>Servi&ccedil;o</th><th>Qtd</th><th>Tempo(Min)</th><th>Atrasos</th></tr></thead><tbody>'''
                tqtd = 0
                ttempo = 0
                strtempo = ''
                for dtuser in rowrs:
                    if (dtuser[2] <> 0) or (dtuser[3] <> 0):
                        html += u'<tr>'
                        html += u'<td> %s </td>' % unicode((dtuser[0] or '').decode('latin-1'))
                        dadoslink = {'nmserv': unicode((dtuser[1] or '').decode('latin-1')), 'idserv': str(dtuser[4]),
                                     'iduser': dados_user['IDUSER'], 'compt': kwargs['dt_ini']}
                        html += u'<td><a href="detprotserv?iduser=%(iduser)s&dt_ini=%(compt)s&iserv=%(idserv)s"> %(nmserv)s </a>  </td>' % dadoslink
                        html += u'<td> %s </td>' % unicode(dtuser[2])
                        html += u'<td> %s </td>' % (unicode(dtuser[3]) + ' (' + unicode(int(dtuser[3] / 60)) + ' hrs)')
                        html += u'<td> %s </td>' % unicode(dtuser[5])
                        html += u'</tr>'
                        tqtd += dtuser[2]
                        ttempo += dtuser[3]
                strtempo = unicode(ttempo) + ' (' + unicode(int(ttempo / 60)) + ' hrs)'
                html += u'''</tbody><tfoot><tr><td colspan="2">TOTAL</td><td>%(qtd)s</td><td>%(tempo)s</td></tr></tfoot>''' % {
                    'qtd': tqtd, 'tempo': strtempo}
                dtmpl['tb_det'] = html
            # Apontamento de Horas Cadastrados
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select id,TPDESC,QTD_TEMPO,TIPO_TEMPO,DESCONTAR from USER_HRS_DESC
            where COMPT= '%(dtini)s' and USUARIO='%(nome)s'
            ''' % {'nome': dados_user['NOME'], 'id_depto': dados_user['ID_DEPTO'], 'dtini': strdataini,
                   'dtfim': strdatafim})
            rowrs = cr.fetchall()
            if rowrs:
                html = u'''<thead><tr><th>Descri&ccedil;&atilde;o</th><th>Tempo</th><th>Tipo</th><th>Descontar</th></tr></thead><tbody>'''
                for dtapont in rowrs:
                    html += u'<tr>'
                    html += u'<td> %s </td>' % contesputil.to_utf(dtapont[1])
                    html += u'<td> %s </td>' % contesputil.to_utf(dtapont[2])
                    html += u'<td> %s </td>' % contesputil.to_utf(dtapont[3])
                    html += u'<td> %s </td>' % contesputil.to_utf(dtapont[4])
                    html += u'</tr>'
                html += u'''</tbody>'''
                dtmpl['tb_detapont'] = html

            # Ligacoes Solicitadas
            # cr=contesputil.ret_cursor('fb')
            # cr.execute('''select tellista.empresacont,count(1) as qtd from tellista
            #		where tellista.requisitante= '%(nome)s'
            #		 and tellista.datlig >= '%(dtini)s'
            #		 and tellista.datlig < '%(dtfim)s'
            #
            #		group by tellista.empresacont
            #		order by tellista.empresacont
            # ''' %  { 'nome':dados_user['NOME'] ,'dtini' : strdataini , 'dtfim' : strdatafim  }  )
            # rowrs = cr.fetchall()
            # if rowrs:
            #	html=u'''<thead><tr><th>Para</th><th>Qtd</th><tbody>'''
            #	for dt in rowrs:
            #		html +=u'<tr>'
            #		html +=u'<td> %s </td>' % unicode(dt[0])
            #		html +=u'<td> %s </td>' % unicode(dt[1])
            #		html +=u'</tr>'
            #	html += u'''</tbody>'''
            #	dtmpl['tb_detlig']=html

            # Ligacoes Recebidas
            # cr=contesputil.ret_cursor('fb')
            # cr.execute('''select lrenome,count(1) as qtd from LIGRECEBIDAS
            #		where lrefalarcom= '%(nome)s'
            #		 and lredata >= '%(dtini)s'
            #		 and lredata < '%(dtfim)s'
            #		 and lretipolig <>'P'
            #		group by lrenome
            #		order by lrenome
            # ''' %  { 'nome':dados_user['NOME'] ,'dtini' : strdataini , 'dtfim' : strdatafim  }  )
            # rowrs = cr.fetchall()
            # if rowrs:
            #	html=u'''<thead><tr><th>De</th><th>Qtd</th><tbody>'''
            #	for dt in rowrs:
            #		html +=u'<tr>'
            #		html +=u'<td> %s </td>' % unicode(dt[0].decode('latin-1','replace'))
            #		html +=u'<td> %s </td>' % unicode(dt[1])
            #		html +=u'</tr>'
            #	html += u'''</tbody>'''
            #	dtmpl['tb_detligrec']=html

            # Emails
            cr = contesputil.ret_cursor('fb')
            cr.execute('''select email.nmcliente, EMAIL.contato,count(1) AS QTD from email
                        where email.dataemissao >= '%(dtini)s'
                        and email.dataemissao < '%(dtfim)s'
                        and email.usuario = '%(nome)s'
                        group by email.nmcliente, EMAIL.contato
            ''' % {'nome': dados_user['NOME'], 'dtini': strdataini, 'dtfim': strdatafim})
            rowrs = cr.fetchall()
            if rowrs:
                html = u'''<thead><tr><th>Para</th><TH>Email</TH><th>Qtd</th><tbody>'''
                for dtapont in rowrs:
                    html += u'<tr>'
                    if dtapont[0]:
                        html += u'<td> %s </td>' % unicode(dtapont[0].decode('latin-1', 'replace'))
                    else:
                        html += u'<td>  </td>'
                    html += u'<td> %s </td>' % unicode(dtapont[1])
                    html += u'<td> %s </td>' % unicode(dtapont[2])
                    html += u'</tr>'
                html += u'''</tbody>'''
                dtmpl['tb_detemail'] = html
        # Prot por cliente
        cr = contesputil.ret_cursor('fb')
        cr.execute('''select cliente.razao,TPSERV.idtpserv AS  ID_SERV,tpserv.nome as serv_nome
            ,count(1) AS total_qtd
            ,sum(coalesce( servicos2.tempo_gasto,0)) AS total_tempo
             from servicos2
                        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
                        left join tpserv on tpserv.idtpserv=servicos2.idservico
                        left join CLIENTE on CLIENTE.codigo=protocolo2.codclirep
                        where protocolo2.emissao >= '%(dtini)s'
                         and protocolo2.emissao < '%(dtfim)s'
                         and protocolo2.status in ('B','N')
                         and servicos2.idservico is not null
                         and SERVICOS2.userrealiz = '%(nome)s'
                         and servicos2.tempo_gasto <> 0
                        group by cliente.razao ,TPSERV.idtpserv,tpserv.nome
            union all
            select cliente.razao ,TPSERV.idtpserv,tpserv.nome as serv_nome
            ,count(1) AS total_qtd
            ,sum(coalesce( servicos2.tempo_useraux,0)) AS total_tempo
             from servicos2
                        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
                        left join tpserv on tpserv.idtpserv=servicos2.idservico
                        left join CLIENTE on CLIENTE.codigo=protocolo2.codclirep
                        where protocolo2.emissao >= '%(dtini)s'
                         and protocolo2.emissao < '%(dtfim)s'
                         and protocolo2.status in ('B','N')
                         and servicos2.idservico is not null
                         and SERVICOS2.useraux = '%(nome)s'
                         and servicos2.tempo_useraux <> 0
                        group by  cliente.razao ,TPSERV.idtpserv,tpserv.nome
        ''' % {'nome': dados_user['NOME'], 'dtini': strdataini, 'dtfim': strdatafim})
        rowrs = cr.fetchall()
        if rowrs:
            html = u'''<thead><tr><th>Cliente</th><TH>Servico</TH><th>Qtd</th><th>Tempo</th><tbody>'''
            for dtapont in rowrs:
                html += u'<tr>'
                html += u'<td> %s </td>' % (dtapont[0] or '').decode('latin-1', 'replace')
                html += u'<td> %s </td>' % unicode(dtapont[2].decode('latin-1', 'replace'))
                html += u'<td> %s </td>' % unicode(dtapont[3])
                html += u'<td> %s </td>' % unicode(dtapont[4])
                html += u'</tr>'
            html += u'''</tbody>'''
            dtmpl['tb_protpcli'] = html

        dtmpl['seluser'] = ushtml
        dtmpl['scompt'] = selcompt
        dtmpl['dados_user'] = dados_user
        dtmpl['session'] = cherrypy.session
        return tmpl.render(dtmpl)

    # return tmpl.render(seluser=ushtml, scompt=selcompt,tb_det=html)

    @cherrypy.expose
    def relexec(self, **kwargs):
        print kwargs
        nmrel = ''
        reltipo = ''
        relsql = ''
        rowrel = list()
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                    </head>
                <body>
                <h1> SQL %(sql)s </h1>
                ''' % kwargs
        if kwargs.has_key('nmrel'):
            nmrel = kwargs['nmrel']
        if kwargs.has_key('sql'):
            relsql = kwargs['sql']

        if (relsql <> ''):
            print relsql
            contesputil.execsql(relsql, 'fb')
            html += ' SQL FINALIZADA '
        return html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def jsmsgulttodos(self, **kwargs):
        cursordo = contesputil.ret_cursor('fb')
        cursordo.execute('''SELECT first 1 msg,EXTRACT(day FROM data)||'/'
                || EXTRACT(month FROM data)||'/'
                || EXTRACT(year  FROM data)||' '
                || EXTRACT(HOUR   FROM data)||':'
                || EXTRACT(MINUTE   FROM data) as dtcad
                 from MSGTODOS order by msgtodos.data desc ''')
        rowrel = contesputil.dictcursor(cursordo)
        return rowrel

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def jsrel(self, **kwargs):
        print kwargs
        nmrel = ''
        reltipo = ''
        relsql = ''
        rowrel = list()
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                    </head>
                <body>
                <h1> relatorio %(nmrel)s </h1>
                ''' % kwargs
        if kwargs.has_key('nmrel'):
            nmrel = kwargs['nmrel']
        cursordo = contesputil.ret_cursor('fb')
        cursordo.execute('''
         select rel_select.tp_con,rel_select.SQL_STR from rel_select
         left join rel_inf on rel_inf.id=rel_select.id_rel_inf
         where rel_inf.nome = '%s' ''' % nmrel)
        descdo = cursordo.description
        rowr = cursordo.fetchone()
        if rowr:
            reltipo = rowr[0]
            relsql = rowr[1] % kwargs

        html += relsql
        if len(relsql) > 3:
            print relsql
            cursorrel = contesputil.ret_cursor(reltipo)
            # try :
            cursorrel.execute(relsql.encode('ascii'))
            # except :
            #	print html.encode('iso8859-1')
            #	print '<H1>ERRO SQL:</H1>'
            #	print relsql
            #	print sys.
            descrel = cursorrel.description
            # rowrel = cursorrel.fetchall()
            rowrel = contesputil.dictcursor(cursorrel)
            cursorrel.close()
        return rowrel

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def jsrel2(self, **kwargs):
        print kwargs
        nmrel = ''
        reltipo = ''
        relsql = ''
        rowrel = list()
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                    </head>
                <body>
                <h1> relatorio %(nmrel)s </h1>
                ''' % kwargs
        if kwargs.has_key('nmrel'):
            nmrel = kwargs['nmrel']
        cursordo = contesputil.ret_cursor('fb')
        cursordo.execute('''
         select rel_select.tp_con,rel_select.SQL_STR from rel_select
         left join rel_inf on rel_inf.id=rel_select.id_rel_inf
         where rel_inf.nome = '%s' ''' % nmrel)
        descdo = cursordo.description
        rowr = cursordo.fetchone()
        if rowr:
            reltipo = rowr[0]
            relsql = rowr[1] % kwargs

        html += relsql
        if len(relsql) > 3:
            # print relsql
            cursorrel = contesputil.ret_cursor(reltipo)
            # try :
            cursorrel.execute(relsql.encode('ascii'))
            # except :
            #	print html.encode('iso8859-1')
            #	print '<H1>ERRO SQL:</H1>'
            #	print relsql
            #	print sys.
            descrel = cursorrel.description
            # rowrel = cursorrel.fetchall()
            rowrel = contesputil.dictcursor2(cursorrel)
            cursorrel.close()
        return rowrel

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def jsrel3(self, **kwargs):
        print kwargs
        nmrel = ''
        reltipo = ''
        relsql = ''
        rowrel = list()
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                    </head>
                <body>
                <h1> relatorio %(nmrel)s </h1>
                ''' % kwargs
        if kwargs.has_key('nmrel'):
            nmrel = kwargs['nmrel']
        cursordo = contesputil.ret_cursor('fb')
        cursordo.execute('''
         select rel_select.tp_con,rel_select.SQL_STR from rel_select
         left join rel_inf on rel_inf.id=rel_select.id_rel_inf
         where rel_inf.nome = '%s' ''' % nmrel)
        descdo = cursordo.description
        rowr = cursordo.fetchone()
        if rowr:
            reltipo = rowr[0]
            relsql = rowr[1] % kwargs

        html += relsql
        if len(relsql) > 3:
            print relsql
            cursorrel = contesputil.ret_cursor(reltipo)
            # try :
            cursorrel.execute(relsql.encode('ascii'))
            # except :
            #	print html.encode('iso8859-1')
            #	print '<H1>ERRO SQL:</H1>'
            #	print relsql
            #	print sys.
            descrel = cursorrel.description
            # rowrel = cursorrel.fetchall()
            rowrel = contesputil.dictcursor2(cursorrel)
            cursorrel.close()
        return {'items': rowrel}

    @cherrypy.expose
    def rel(self, **kwargs):
        print kwargs
        nmrel = ''
        reltipo = ''
        relsql = ''
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                    </head>
                <body>
                <h1> relatorio %(nmrel)s </h1>
                ''' % kwargs
        if kwargs.has_key('nmrel'):
            nmrel = kwargs['nmrel']
        cursordo = contesputil.ret_cursor('fb')
        cursordo.execute('''
         select rel_select.tp_con,rel_select.SQL_STR from rel_select
         left join rel_inf on rel_inf.id=rel_select.id_rel_inf
         where rel_inf.nome = '%s' ''' % nmrel)
        descdo = cursordo.description
        rowr = cursordo.fetchone()
        if rowr:
            reltipo = rowr[0]
            relsql = rowr[1] % kwargs

        html += relsql
        if len(relsql) > 3:
            cursorrel = contesputil.ret_cursor(reltipo)
            # try :
            cursorrel.execute(relsql.encode('ascii'))
            # except :
            #	print html.encode('iso8859-1')
            #	print '<H1>ERRO SQL:</H1>'
            #	print relsql
            #	print sys.
            descrel = cursorrel.description
            rowrel = cursorrel.fetchall()
            html = html + '<table align="center"  border="1" > '
            html = html + '<tr>'
            for col in range(len(descrel)):
                html = html + "<td> %s </td>" % (descrel[col][0])
            html = html + '</tr>'

            for row in rowrel:
                html = html + '<tr>'
                for col in range(len(descrel)):
                    if type(row[col]) == str:
                        html = html + "<td> %s </td> " % row[col].decode('latin-1')
                    elif row[col] is None:
                        html = html + "<td> &nbsp; </td> "
                    elif type(row[col]) == str:
                        html = html + "<td> %s </td> " % row[col].decode('latin-1')
                    elif type(row[col]) is float:
                        html = html + "<td> %s </td> " % locale.format("%.2f", row[col], grouping=True, monetary=True)
                    elif descrel[col][1] is decimal.Decimal:
                        html = html + "<td align=right > %s </td> " % locale.format("%.2f", row[col], grouping=True,
                                                                                    monetary=True)
                    elif type(row[col]) is datetime.datetime:
                        html = html + "<td> %s </td> " % row[col].strftime("%d/%m/%Y")
                    else:
                        html = html + "<td> %s </td> " % (row[col])
                html = html + '</tr>'

            html = html + '''</table>  '''
            cursorrel.close()
        html += '</body> '
        return html

    @cherrypy.expose
    def roteiro(self, **kwargs):
        html = '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
                <html>
                    <head>
                     <meta name="viewport" content="user-scalable=no, width=device-width">
                    <style type="text/css">
                    @media screen {
  body {
    font-size: 18px;
  }
}

@media (max-width: 480px) {
  body {
    font-size: 18px;
  }
}
</style>
                    </head>
                <body>
                
                ''' % kwargs

        if kwargs.has_key('area'):
            nmarea = kwargs['area']
            html = html + '<h1> Roteiro Contesp Area:%(area)s</h1> ' % {'area': nmarea}
            relsql = '''
            SELECT IDROT,usuario,nome,responsavel,codprot,obs area,ordem,boy FROM roteiro2
             WHERE ROTEIRO2.EXECUTAR = CURRENT_DATE
              AND ROTEIRO2.STATUS='N'
              and area = %(area)s
             ORDER BY aREA,ordem
            ''' % {'area': nmarea}
            cursorrel = contesputil.ret_cursor('fb')

            cursorrel.execute(relsql.encode('ascii'))
            descrel = cursorrel.description
            rowrel = cursorrel.fetchall()
            html = html + '<table align="center"  border="1" > '
            html = html + '<tr>'
            html = html + "<td> # </td>"
            html = html + "<td> Nome </td>"
            html = html + "<td> De </td>"
            html = html + "<td> Para </td>"
            html = html + "<td> L.Prot. </td>"
            html = html + "<td> OBS </td>"
            html = html + "<td> Boy </td>"
            html = html + '</tr>'

            for row in rowrel:
                linkr = ' <a href="roteiro?rot=%(id)s"> %(id)s </a> ' % {'id': row[0]}
                html = html + '<tr>'
                html = html + "<td> %s </td> " % linkr
                html = html + "<td> %s </td> " % str(row[1] or '&nbsp;').decode('latin-1')
                html = html + "<td> %s </td> " % str(row[2] or '&nbsp;').decode('latin-1', replace)
                html = html + "<td> %s </td> " % str(row[3] or '&nbsp;').decode('latin-1')
                html = html + "<td> %s </td> " % str(row[4] or '&nbsp;').decode('latin-1')
                html = html + "<td> %s </td> " % str(row[5] or '&nbsp;').decode('latin-1')
                html = html + "<td> %s </td> " % str(row[8] or '&nbsp;').decode('latin-1')
                html = html + '</tr>'
            html = html + '''</table>  '''
        else:
            html = html + '<h1> Roteiro Contesp </h1> '
            relsql = '''
            SELECT ROTEIRO2.area,roteiro2.boy,COUNT(1) FROM roteiro2
             WHERE ROTEIRO2.EXECUTAR = CURRENT_DATE
              AND ROTEIRO2.STATUS='N'
            GROUP BY ROTEIRO2.AREA,ROTEIRO2.BOY
             ORDER BY ROTEIRO2.AREA,ROTEIRO2.BOY
            '''
            cursorrel = contesputil.ret_cursor('fb')

            cursorrel.execute(relsql.encode('ascii'))
            descrel = cursorrel.description
            rowrel = cursorrel.fetchall()
            html = html + '<table align="center"  border="1" > '
            html = html + '<tr>'
            html = html + "<td> Area </td>"
            html = html + "<td> Mensageiro </td>"
            html = html + "<td> Qtd </td>"
            html = html + '</tr>'

            for row in rowrel:
                link = ' <a href="roteiro?area=%(id)s"> %(id)s </a> ' % {'id': row[0]}
                html = html + '<tr>'
                html = html + "<td> %s </td> " % link
                html = html + "<td> %s </td> " % str(row[1] or '&nbsp;').decode('latin-1')
                html = html + "<td> %s </td> " % row[2]
                html = html + '</tr>'

            html = html + '''</table>  '''
        html += '</body> '
        return html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totequipe2(self, **kwargs):

        lst_usr = list()
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''
        select grupousr.descricao as equipe, usuario.iduser, COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        ,tpserv.nome
        ,cast(sum(coalesce( servicos2.qtd_item,0)) as integer) as qtd
        ,sum(coalesce( servicos2.tempo_gasto,0)) AS TOTAL
        from servicos2
        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
        left join tpserv on tpserv.idtpserv=servicos2.idservico
        left join depto on depto.nomedepartamento=protocolo2.deptouser
        left join usuario on usuario.nome=COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        left join sp_grupousr_participante_vig(usuario.iduser,'%(dtini)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id = gp.IDGRP
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and depto.id_depto = %(setor)s
         and tpserv.idtpserv is not null
        group by grupousr.descricao,usuario.iduser, COALESCE(SERVICOS2.userrealiz, protocolo2.usuario),tpserv.nome
        ORDER by grupousr.descricao,COALESCE(SERVICOS2.userrealiz, protocolo2.usuario),tpserv.nome,usuario.iduser
        
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # rowsetfb = contesputil.dictcursor(cursorfb)
        linha = '';
        for row in rowsetfb:
            linha = unicode(row[0])
            # linha+='</td><td>'+unicode(row[1])
            linha += '</td><td>' + unicode(row[2])
            linha += '</td><td>' + unicode(row[3])
            linha += '</td><td>' + unicode(row[4])
            linha += '</td><td>' + unicode(row[5])
            lst_serv.append(linha)
        return lst_serv

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totequipe(self, **kwargs):

        lst_usr = list()
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''
        select grupousr.descricao as equipe, COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        ,cast(COUNT(1) as integer) as qtd
        ,sum(coalesce( servicos2.tempo_gasto,0)) AS TOTAL
        from servicos2
        inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
        left join tpserv on tpserv.idtpserv=servicos2.idservico
        left join depto on depto.nomedepartamento=protocolo2.deptouser
        left join usuario on usuario.nome=COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        left join sp_grupousr_participante_vig(usuario.iduser,'%(dtini)s') gp on gp.IDUSR=usuario.iduser
        left join grupousr on grupousr.id = gp.IDGRP
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and depto.id_depto = %(setor)s
         and tpserv.idtpserv is not null
        group by grupousr.descricao, COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        ORDER by grupousr.descricao,COALESCE(SERVICOS2.userrealiz, protocolo2.usuario)
        
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # rowsetfb = contesputil.dictcursor(cursorfb)
        linha = '';
        for row in rowsetfb:
            linha = unicode(row[0])
            linha += '</td><td>' + unicode(row[1])
            linha += '</td><td>' + unicode(row[2])
            lst_serv.append(linha)
        return lst_serv

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totuser(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        qtddias = contesputil.diasuteis(datainicial.date(), diasfinal) + 1
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        lst_usr = self.lista_user2(strdataini)
        # print lst_usr
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select * from DESEMPENHO_USER('%(dtini)s','%(dtfim)s', %(setor)s )
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # rowsetfb = contesputil.dictcursor(cursorfb)
        linha = '';
        for row in rowsetfb:
            usuarios = [item for item in lst_usr if item['NOME'] == row[0]]
            tempo = 0.0
            equipe = ''
            tempo_desc = 0
            meta = 0
            if usuarios:
                # print usuarios
                tempo = (usuarios[0]['HRS_META'] or 0.0)
                if (datainicial.month == hj.month) and (datainicial.year == hj.year):
                    tempo_desc = 0
                else:
                    tempo_desc = (usuarios[0]['TMP_DESC'] or 0)
                equipe = (usuarios[0]['GRP_NOME'] or '')
                tmp_gasto = (row[1] or 0) / 60
            linha = unicode(row[0])
            linha += '</td><td>' + unicode(equipe)
            linha += '</td><td>' + unicode(tmp_gasto)
            # linha+='</td><td>'+unicode((tempo*qtddias))+'(Min) / '+unicode((tempo*qtddias)/60)+'(Hrs)'
            meta = int(((tempo * qtddias) - tempo_desc) / 60)
            if meta < 0:
                meta = 0
            if meta > 168:
                meta = 168
            linha += '</td><td>' + unicode(meta)
            lst_serv.append(linha)
        return lst_serv

    def cliente_tributacao(self, codcli, dt_inicial):
        strdatini = dt_inicial.strftime("%Y-%m-%d")
        tributacao = u'Branco'
        sql = ''' select cliente.cli_coddominio from cliente 
            where cliente.codigo= %(cod)d
            ''' % {'cod': codcli}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql.encode('ascii'))
        clientes = cursorfb.fetchall()
        for clidominio in clientes:
            if clidominio[0]:
                sql = ''' SELECT TOP 1 bethadba.EFPARAMETRO_VIGENCIA.RFED_PAR  
                    FROM bethadba.EFPARAMETRO_VIGENCIA    
                    WHERE CODI_EMP= %(codcli)s
                        AND vigencia_par <= '%(dtini)s'
                    order by vigencia_par desc
                    ''' % {'codcli': clidominio[0], 'dtini': strdatini}
                cursorfb = contesputil.ret_cursor('do')
                cursorfb.execute(sql.encode('ascii'))
                dbtribut = cursorfb.fetchall()
                for codtrib in dbtribut:
                    if codtrib[0] == 5:
                        tributacao = u'Presumido'
                    elif codtrib[0] == 1:
                        tributacao = u'Real'
                    else:
                        tributacao = u'Simples'
        return tributacao

    def eq_clientes(self, id_equipe, dt_inicial):
        lst_cli = list()
        strdatini = dt_inicial.strftime("%Y-%m-%d")

        if type(id_equipe) == int:
            ideqatual = id_equipe
        else:
            ideqatual = id_equipe or 0

        sql = ''' select cliente.cli_coddominio from grupousr_cliente
            left join cliente on cliente.codigo=grupousr_cliente.idcliente
            where grupousr_cliente.id_grupousr = %(ideq)d
            ''' % {'ideq': ideqatual}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql.encode('ascii'))
        clientes = cursorfb.fetchall()
        for clidominio in clientes:
            if clidominio[0]:
                sql = ''' SELECT TOP 1 bethadba.EFPARAMETRO_VIGENCIA.RFED_PAR  
                    FROM bethadba.EFPARAMETRO_VIGENCIA    
                    WHERE CODI_EMP= %(codcli)s
                        AND vigencia_par <= '%(dtini)s'
                    order by vigencia_par desc
                    ''' % {'codcli': clidominio[0], 'dtini': strdatini}
                cursorfb = contesputil.ret_cursor('do')
                cursorfb.execute(sql.encode('ascii'))
                dbtribut = cursorfb.fetchall()
                tributacao = u'Branco'
                for codtrib in dbtribut:
                    if codtrib[0] == 5:
                        tributacao = u'Presumido'
                    elif codtrib[0] == 1:
                        tributacao = u'Real'
                    else:
                        tributacao = u'Simples'

                tottrib = next((x for x in lst_cli if unicode(x['tributacao']) == unicode(tributacao)), None)
                if tottrib == None:
                    tottrib = {'tributacao': tributacao, 'qtd': 0}
                    lst_cli.append(tottrib)
                tottrib['qtd'] += 1
        lst_cli.sort(key=lambda k: k['tributacao'])
        return lst_cli

    def busca_qtd(self, nm_user, dt_inicial, numdepto):
        qtdinf = 0
        datainicial = datetime.datetime.strptime(dt_inicial, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')

        if (int(numdepto) or 0) in [3, 4]:
            sql = ''' select sum(qtd) from SERV_QTDLANC 
             where COMPETENCIA = '%(dtini)s'
              and USER_CONTROLE ='%(nmuser)s'
              and IDDEPTO = %(setor)s 
                ''' % {'dtini': dt_inicial, 'nmuser': nm_user, 'setor': numdepto}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql.encode('ascii'))
            rowsetfb = cursorfb.fetchall()
            for row in rowsetfb:
                qtdinf += (row[0] or 0)
        if (int(numdepto) or 0) == 2:
            sql = ''' select  sum(coalesce(total,0)) as total from
            (
            select sum(coalesce( servicos2.qtd_item,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico =36
             and (( SERVICOS2.userrealiz ='%(nmuser)s' ) or (SERVICOS2.useraux ='%(nmuser)s'))
            )
                ''' % {'dtini': dt_inicial, 'dtfim': strdatafim, 'nmuser': nm_user, 'setor': numdepto}
            s2 = '''
            union all
            select sum(coalesce( servicos2.qtd_item,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouseraux =  %(setor)s
             and servicos2.idservico =36
             and SERVICOS2.useraux ='%(nmuser)s'
             '''
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql.encode('ascii'))
            rowsetfb = cursorfb.fetchall()
            for row in rowsetfb:
                qtdinf += (row[0] or 0)
        return qtdinf

    def busca_qtdcli(self, nm_user, dt_inicial, numdepto):
        qtdinf = 0
        datainicial = datetime.datetime.strptime(dt_inicial, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        lstqtd = list()
        if (int(numdepto) or 0) in [3, 4]:
            sql = ''' select SERV_QTDLANC.COD_CONTROLE,cliente.razao,sum(qtd) from SERV_QTDLANC 
            left join cliente on cliente.codigo=SERV_QTDLANC.cod_controle
             where SERV_QTDLANC.COMPETENCIA = '%(dtini)s'
              and SERV_QTDLANC.USER_CONTROLE ='%(nmuser)s'
              and SERV_QTDLANC.IDDEPTO = %(setor)s 
             group by SERV_QTDLANC.COD_CONTROLE,cliente.razao
                ''' % {'dtini': dt_inicial, 'nmuser': nm_user, 'setor': numdepto}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql.encode('ascii'))
            rowsetfb = cursorfb.fetchall()
            for row in rowsetfb:
                cli = dict()
                cli['codigo'] = row[0]
                cli['razao'] = unicode(row[1].decode('latin-1'))
                cli['qtd'] = row[2]
                lstqtd.append(cli)
        if (int(numdepto) or 0) == 2:
            sql = ''' select codigo,razao, sum(coalesce(total,0)) as total from
            (
            select cliente.codigo,cliente.razao,sum(coalesce( servicos2.qtd_item,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join cliente on cliente.codigo=protocolo2.CODCLIREP
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouser = %(setor)s
             and servicos2.idservico =36
             and (( SERVICOS2.userrealiz ='%(nmuser)s' ) or (SERVICOS2.useraux ='%(nmuser)s'))
            group by  cliente.codigo,cliente.razao
            ) group by codigo , razao
                ''' % {'dtini': dt_inicial, 'dtfim': strdatafim, 'nmuser': nm_user, 'setor': numdepto}
            s2 = '''
            union all
            select cliente.codigo,cliente.razao, sum(coalesce( servicos2.qtd_item,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join cliente on cliente.codigo=protocolo2.CODCLIREP
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and servicos2.iddeptouseraux =  %(setor)s
             and servicos2.idservico =36
             and SERVICOS2.useraux ='%(nmuser)s'
             group by  cliente.codigo,cliente.razao,
             '''
            cursorfb = contesputil.ret_cursor('fb')

            cursorfb.execute(sql.encode('ascii'))
            rowsetfb = cursorfb.fetchall()
            for row in rowsetfb:
                totpcli = dict()
                totpcli['codigo'] = row[0]
                totpcli['razao'] = unicode(row[1].decode('latin-1'))
                totpcli['qtd'] = row[2]
                lstqtd.append(totpcli)
        return lstqtd

    def grav_totuser(self, lst_tot):
        if len(lst_tot) > 0:
            sql = '''delete from USER_QTD_LANC
                where ID_DEPTO=%(setor)s
                and COMPETENCIA='%(compt)s'
            ''' % lst_tot[0]
            contesputil.execsql(sql.encode('ascii'), 'fb')

        for us in lst_tot:
            sql = '''insert into USER_QTD_LANC
                ( NM_USER ,EQUIPE ,TEMPO_TRAB ,META ,QTD ,ID_DEPTO ,COMPETENCIA )
                values
                ( '%(nome)s','%(equipe)s', %(hrs_trab)s, %(meta)s, %(qtd)s,%(setor)s,'%(compt)s'   )
            ''' % us
            contesputil.execsql(sql.encode('latin-1', 'replace'), 'fb')

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totuser2(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]

        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select * from DESEMPENHO_USER('%(dtini)s','%(dtfim)s', %(setor)s )
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # rowsetfb = contesputil.dictcursor(cursorfb)
        # print rowsetfb
        for row in rowsetfb:
            linha = dict();
            usuarios = self.get_user_meta_dados(nmuser=row[0], dtini=datainicial)
            tempo = 0.0
            equipe = ''
            ideqp = 0
            tempo_desc = 0
            tmp_gasto = tmp_gasto = (row[1] or 0) / 60
            iduser = 0
            tempo_meta = 0
            if usuarios:
                tempo = (usuarios[0]['HRS_META'] or 0.0)
                tempo_desc = (usuarios[0]['TMP_DESC'] or 0)
                tempo_meta = (usuarios[0]['TMP_META'] or 0)
                iduser = (usuarios[0]['IDUSER'] or 0)
                equipe = u''
                if usuarios[0]['GRP_NOME']:
                    equipe = usuarios[0]['GRP_NOME']
                ideqp = (usuarios[0]['GRP_ID'] or 0)

            linha['nome'] = unicode(row[0])
            linha['iduser'] = iduser
            linha['di_ini'] = kwargs['dt_ini']
            linha['equipe'] = unicode(equipe)
            linha['id_eqp'] = ideqp
            linha['hrs_trab'] = tmp_gasto
            linha['meta'] = int(tempo_meta / 60)
            if ((linha['meta'] == 0) and (linha['hrs_trab'] > 0)):
                linha['meta'] = linha['hrs_trab']
            html_link = '<a href="detuser?iduser=%(id_user)s&dt_ini=%(dini)s"> %(nome)s </a> ' % {'nome': linha['nome'],
                                                                                                  'id_user': linha[
                                                                                                      'iduser'],
                                                                                                  'dini': kwargs[
                                                                                                      'dt_ini']}
            linha['link_user'] = html_link
            html_link = '<a href="det_equipe?ideqp=%(id_eqp)s&dt_ini=%(dini)s"> %(nome)s </a> ' % {
                'nome': linha['equipe'], 'id_eqp': linha['id_eqp'], 'dini': kwargs['dt_ini']}
            linha['link_eqp'] = html_link
            linha['qtd'] = self.busca_qtd(linha['nome'], strdataini, numdepto)
            linha['setor'] = numdepto
            linha['compt'] = strdataini
            lst_serv.append(linha)
        self.grav_totuser(lst_serv)
        lst_serv.sort(key=lambda k: k['equipe'])
        return lst_serv

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totproc(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        dataanterior = contesputil.subtract_one_month(datainicial)
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()
        qtddias = contesputil.diasuteis(datainicial.date(), diasfinal)
        qtddiasmes = contesputil.diasuteis(datainicial.date(), datafinal.date())

        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        strdataant = dataanterior.strftime("%d.%m.%Y")
        strpcompt = datainicial.strftime("%m/%Y").replace('/', '%2F')
        lst_usr = self.lista_user2(strdataini)
        # Pega Ausencias
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select usuario,sum(tempo_total) as qtd from user_hrs_desc
         where compt='%(dtini)s' and ((descontar ='N') or (descontar is null))
         group by usuario
            ''' % {'dtini': strdataini, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        ausencias = contesputil.dictcursor(cursorfb)
        # Pega total por processo
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select * from DESEMPENHO_PROC('%(dtini)s','%(dtfim)s', %(setor)s )
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        lstp = list()
        processos = list()
        if (numdepto == 3) or (numdepto == 4):
            processos.append(unicode('LANCAMENTOS'))
        if numdepto == 3:
            processos.append(unicode('CONFERE'))
        for row in rowsetfb:
            nmpprs = u''
            if row[1]:
                nmpprs = unicode(row[1].decode('latin-1'))
            lstp.append(nmpprs)
        proclist = [processos.append(p) for p in lstp if p not in processos]
        hrslimite = (84 / qtddiasmes) * qtddias
        equipes = list()
        # Carrega dados pela procedure
        for row in rowsetfb:
            usuarios = [item for item in lst_serv if item['nome'] == unicode(row[0])]
            if usuarios:
                seluser = usuarios[0]
            else:
                seluser = dict()
                lst_serv.append(seluser)
                u = [item for item in lst_usr if item['NOME'] == row[0]]
                if u:
                    usertrab = u[0]
                    seluser['nome'] = unicode(row[0])
                    seluser['equipe'] = unicode((usertrab['GRP_NOME'] or '').decode('latin-1'))
                    seluser['iduser'] = unicode(usertrab['IDUSER'] or '0')
                    seluser['eq_id'] = usertrab['GRP_ID']
                    seluser['dat_inicio'] = usertrab['DAT_INICIO']
                    seluser['dat_fim'] = usertrab['DAT_FIM']

                    tempo = (usertrab['HRS_META'] or 0.0)
                    tempo_desc = (usertrab['TMP_DESC'] or 0)
                    meta = int((usertrab['TMP_META'] or 0) / 60)
                    if meta < 0:
                        meta = 0
                    seluser['meta'] = meta
                    seluser['hrs_limite'] = hrslimite
                else:
                    inatuser = self.lista_user_nm(strdataini, row[0])[0]

                    seluser['nome'] = unicode(row[0])
                    seluser['equipe'] = unicode((inatuser['GRP_NOME'] or '').decode('latin-1'))
                    seluser['iduser'] = unicode(inatuser['IDUSER'] or '0')
                    seluser['eq_id'] = inatuser['GRP_ID']
                    seluser['dat_inicio'] = inatuser['DAT_INICIO']
                    seluser['dat_fim'] = inatuser['DAT_FIM']
                    tempo = (0.0)
                    tempo_desc = (0)
                    meta = 0
                    seluser['meta'] = meta
                    seluser['hrs_limite'] = 0
                if seluser['meta'] > 168:
                    seluser['meta'] = 168

                seluser['ausencia'] = next((x['QTD'] for x in ausencias if x['USUARIO'] == row[0]), 0)
                seluser['qtdl'] = self.busca_qtd(seluser['nome'], strdataini, numdepto)
            # Grava os valores dos processos no dic usuario
            tmpnmproc = u''
            if type(row[1]) == str:
                try:
                    tmpnmproc = unicode(row[1].decode('latin-1'))
                except UnicodeDecodeError, UnicodeEncodeError:
                    tmpnmproc = u''
            seluser[tmpnmproc] = row[2]

        # Calcula total por equipe
        for seluser in lst_serv:
            eq_atual = next((e for e in equipes if e['eq_id'] == seluser['eq_id']), None)
            if type(seluser['eq_id']) == int:
                ideq = seluser['eq_id']
            else:
                ideq = 0
            if eq_atual == None:
                eq_atual = {'nm_equipe': seluser['equipe'],
                            'eq_id': seluser['eq_id'],
                            'meta': 0,
                            'qtdl': 0,
                            'total': 0,
                            'totant': 0,
                            'totausencia': 0,
                            'qtd_clientes': list(),
                            'html_link': '<a href="/det_equipe?ideqp=' + str(
                                ideq or 0) + '&dt_ini=' + strpcompt + '">' + seluser['equipe'] + '</a>'}
                equipes.append(eq_atual)
            usertotal = 0.0
            for proc in processos:
                if seluser.has_key(proc):
                    vlr = (seluser[proc] or '0.0')
                else:
                    vlr = '0'
                usertotal += float(vlr)
            # Se a Meta Igual a zero
            # Mudar meta para tempo trabalhado
            if (seluser['meta'] == 0) and (usertotal > 0):
                seluser['meta'] = int(usertotal / 60)
            eq_atual['meta'] += int(seluser['meta'] or '0')
            eq_atual['total'] += int(usertotal / 60)
            eq_atual['totausencia'] += int(seluser['ausencia'] / 60)
            eq_atual['qtdl'] += (seluser['qtdl'] or 0)

        for eq in equipes:
            if eq['meta'] > 0:
                porcent = eq['total'] * 100 / eq['meta']
            else:
                porcent = 0
            if eq['total'] == eq['meta']:
                eq['cor'] = 'label-success'
                eq['msg'] = 'PARABENS META'
            elif porcent >= 100:
                eq['cor'] = 'label-success'
                eq['msg'] = 'PARABENS'
            elif porcent >= 90:
                eq['cor'] = 'label-warning'
                eq['msg'] = 'DENTRO DO LIMITE'
            else:
                eq['cor'] = 'label-important'
                eq['msg'] = 'ALERTA'

            eq['msg'] = eq['msg'] + ' (' + str(porcent) + '%)'
            eq['qtd_clientes'] = self.eq_clientes(eq['eq_id'], datainicial)
        equipes.sort(key=lambda k: k['nm_equipe'])
        lst_html = list()
        html = ''
        html += '<thead  ><tr class="tblTitle" >'
        html += '<th> NOME </th>'
        print numdepto
        if int(numdepto) == 2:
            html += '<th> QTD Vinc. </th>'
        else:
            html += '<th> QTD Lanç. </th>'
        for proc in processos:
            if proc.encode('utf-8', 'replace') == 'LANCAMENTOS':
                html += '<th>Lanç.</th>'
            else:
                html += '<th class="cproc" >' + proc.encode('utf-8', 'replace') + '</th>'
        html += '</th><th> META </th><th>TOTAL</th><th>DISPONÍVEL</th><th>AUS&EcircNCIAS (hrs)</th><th>RESULTADO</th></tr></thead>  '
        lst_html.append({'linha': html})
        lst_html.append({'linha': '<tbody >'})
        for eq in equipes:
            lserv = [item for item in lst_serv if item['equipe'] == eq['nm_equipe']]
            for i in range(len(lserv)):
                usertotal = 0.0
                linha = lserv[i]
                html_link = '<a href="detuser?iduser=%(id_user)s&dt_ini=%(dini)s"> %(nome)s </a> ' % {
                    'nome': linha['nome'], 'id_user': linha['iduser'], 'dini': kwargs['dt_ini']}
                html_fim = ''

                if linha.has_key('dat_fim'):
                    if linha['dat_fim']:
                        html_fim = '   =>  Desligamento:' + linha['dat_fim'].strftime("%d/%m/%Y")
                html = '<tr >'
                html += '<td> %(slink)s %(dtfim)s </td>' % {'dtfim': html_fim, 'slink': html_link,
                                                            'equipe': linha['equipe'], 'meta': linha['meta']}
                html += '<td> %(qtd)s </td>' % {'qtd': linha['qtdl']}
                for proc in processos:
                    if linha.has_key(proc):
                        vlr = (linha[proc] or '0.0')
                    else:
                        vlr = '0.0'
                    html += '<td> %.2f </td>' % (float(vlr) / 60)
                    usertotal += float(vlr)
                # Imprime total
                html += '<td>%d</td>' % int(linha['meta'])
                html += '<td>%d</td>' % int(usertotal / 60)
                html += '<td>%d</td>' % int(round(linha['meta'] - int(usertotal / 60)))
                html += '<td>%d</td>' % int(linha['ausencia'] / 60)
                # if i==0 :
                #	html +='<td rowspan="%(numeq)s"> <div class="label %(cor)s"   >EQUIPE: %(equipe)s <BR> %(msg)s</div></td>' % { 'equipe':linha['equipe'], 'meta':str(eq['meta']) ,'cor':eq['cor'],'msg':eq['msg'],'numeq':len(lserv)}
                html += '</tr>'
                lst_html.append({'linha': html})
            # print 'Diferenca = '+str((eqtvlr-eqtmeta))
            strqtdcli = '<table border="0" ><tr><div style="font-size: 8px;font-weight: bold;"  >'
            totqtd_clientes = 0
            for trib in eq['qtd_clientes']:
                strqtdcli += '<td style="border-bottom: 4px ;border-left: 0px none ;   ">' + trib[
                    'tributacao'] + ':' + str(trib['qtd']) + ' </td> '
                totqtd_clientes += trib['qtd']
            strqtdcli += '<td style="border-bottom: 4px ;border-left: 0px none ;   ">Total:' + str(
                totqtd_clientes) + ' </td> '
            strqtdcli += ' </div></tr></table>'
            html = '<tr style="border-bottom: 4px solid black;border-left: 0px none ;  background:#CFCFCF ; ">  '

            html += '<td class="tbfont12" > EQUIPE: %(equipe)s  %(clientes)s</td>' % {'equipe': eq['html_link'],
                                                                                      'cor': eq['cor'],
                                                                                      'msg': eq['msg'],
                                                                                      'clientes': strqtdcli}
            html += '<td class="tbfont11" >   %(qtd)d </td>' % {'qtd': eq['qtdl']}
            for proc in processos:
                html += '<td>  </td>'
            html += '<td class="tbfont11" >   %(meta)d </td>' % {'meta': (eq['meta'])}
            html += '<td class="%(fontcor)s" >   %(total)d </td>' % {'total': int(eq['total']), 'fontcor': (
                'tbfont11r' if (int(eq['total']) < eq['meta']) else 'tbfont11')}
            html += '<td class="tbfont11" >   %d </td>' % int(eq['meta'] - eq['total'])
            html += '<td class="tbfont11" >   %d </td>' % int(eq['totausencia'])
            if (linha['equipe'] == 'FISEDSON') or (linha['eq_id'] == 60) or (linha['eq_id'] == 61):
                html += '<td>&nbsp;</td>'
            elif linha['eq_id'] == 55:
                html += '<td > <div class="label"   >EQUIPE: %(equipe)s <BR> EQUIPE DE SUPORTE</div></td>' % {
                    'equipe': linha['equipe'], 'meta': str(eq['meta']), 'cor': eq['cor'], 'msg': eq['msg'],
                    'numeq': len(lserv)}
            else:
                html += '<td > <div class="label %(cor)s"   >EQUIPE: %(equipe)s <BR> %(msg)s</div></td>' % {
                    'equipe': linha['equipe'], 'meta': str(eq['meta']), 'cor': eq['cor'], 'msg': eq['msg'],
                    'numeq': len(lserv)}
            html += '</tr>'

            lst_html.append({'linha': html})
        html = ''
        html += '<tfoot><tr class="tblTitle" >'
        html += '<th> NOME </th>'

        if int(numdepto) == 2:
            html += '<th> QTD Vinc. </th>'
        else:
            html += '<th> QTD Lanç. </th>'
        for proc in processos:
            if proc.encode('utf-8', 'replace') == 'LANCAMENTOS':
                html += '<th>Lanç.</th>'
            else:
                html += '<th >' + proc.encode('utf-8', 'replace') + '</th>'
        html += '</th><th> META </th><th>TOTAL</th><th>DISPONÍVEL</th><th>AUS&EcircNCIAS (hrs)</th><th>RESULTADO</th></tr><tfoot>  '
        lst_html.append({'linha': html})
        html = '<tfoot><tr>'
        ttqtd = 0
        for eq in equipes:
            ttqtd += sum((trib['qtd'] for trib in eq['qtd_clientes']))
        html += '<td class="tbfont14" >  Total <br>Clientes: %(qtd)d </td>' % {'qtd': ttqtd}
        html += '<td class="tbfont11" >  %(qtd)d </td>' % {'qtd': sum((us['qtdl'] for us in lst_serv))}
        for proc in processos:
            html += '<td>  </td>'
        html += '<td class="tbfont11"> %d </td>' % sum((eq['meta'] for eq in equipes if eq['eq_id'] <> 55))
        html += '<td class="tbfont11">%d</td>' % sum((eq['total'] for eq in equipes if eq['eq_id'] <> 55))
        difetmp = dict()
        difetmp['diferenca'] = sum((eq['meta'] - eq['total'] for eq in equipes if eq['eq_id'] <> 55))
        difetmp['hrstrab'] = float(qtddias * 8)
        if difetmp['hrstrab'] > 168:
            difetmp['hrstrab'] = 168.0
        difetmp['dpessoa'] = difetmp['diferenca'] / difetmp['hrstrab']
        if difetmp['dpessoa'] < 0:
            difetmp['emp_ociosos'] = ' Faltam Empreg. '
        else:
            difetmp['emp_ociosos'] = ' Empreg. Ociosos '

        difetmp['qtddias'] = qtddias
        html += '<td class="tbfont11">%(diferenca)d ( Cada empreg. Trab. %(hrstrab)s horas no periodo )<br>( %(dpessoa).2f Empreg. )<br> (%(emp_ociosos)s)</td>' % difetmp
        html += '<td class="tbfont11">%d</td>' % sum((eq['totausencia'] for eq in equipes))
        html += '</tr></tfoot>'
        lst_html.append({'linha': html})
        return lst_html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totcli(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        dataanterior = contesputil.subtract_one_month(datainicial)
        hj = datetime.date.today()
        if (datainicial.month == hj.month) and (datainicial.year == hj.year):
            diasfinal = hj
        else:
            diasfinal = contesputil.ultimodia(datainicial).date()

        numdepto = 0
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        strdataant = dataanterior.strftime("%d.%m.%Y")

        # Pega Todos clientes
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select codigo,razao,status,CLI_CODDOMINIO from cliente order by razao
            ''' % {'dtini': strdataini, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        cli_todos = contesputil.dictcursor(cursorfb)
        # Pega total tempo por cliente
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select * from desempenho_cli_setor('%(dtini)s','%(dtfim)s' )
            ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cursorfb.execute(sql.encode('ascii'))
        cli_tempo = contesputil.dictcursor(cursorfb)

        # Pega total qtd por cliente contabil / fiscal
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select COD_CONTROLE,IDDEPTO , sum(qtd) as qtd from SERV_QTDLANC 
             where COMPETENCIA = '%(dtini)s'
             group by COD_CONTROLE,IDDEPTO 
            ''' % {'dtini': strdataini}
        cursorfb.execute(sql.encode('ascii'))
        cli_qtd = contesputil.dictcursor(cursorfb)

        # Carrega dados pela procedure
        for cli in cli_todos:
            cli['tempo_pessoal'] = next(
                (t['TOTAL'] for t in cli_tempo if ((t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 2))), 0) / 60
            cli['tempo_fiscal'] = next(
                (t['TOTAL'] for t in cli_tempo if (t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 3)), 0) / 60
            cli['tempo_contabil'] = next(
                (t['TOTAL'] for t in cli_tempo if (t['CODCLI'] == cli['CODIGO']) and (t['SETOR'] == 4)), 0) / 60
            cli['tempo_total'] = cli['tempo_pessoal'] + cli['tempo_fiscal'] + cli['tempo_contabil']
            cli['qtd_pessoal'] = next(
                (t['QTD'] for t in cli_qtd if ((t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 2))), 0)
            cli['qtd_fiscal'] = next(
                (t['QTD'] for t in cli_qtd if (t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 3)), 0)
            cli['qtd_contabil'] = next(
                (t['QTD'] for t in cli_qtd if (t['COD_CONTROLE'] == cli['CODIGO']) and (t['IDDEPTO'] == 4)), 0)

        lst_html = list()
        html = ''
        html += '<thead><tr>'
        html += '<th> ID </th>'
        html += '<th> NOME </th>'
        html += '<th> Status </th>'
        html += '<th> Qtd Pessoal </th><th>Lanç Fiscal</th><th>Lanç Contabil</th>'
        html += '<th> Tempo Pessoal </th><th>Tempo Fiscal</th><th>Tempo Contabil</th><th>Tempo Total</th></tr></thead>'
        lst_html.append({'linha': html})

        for cli in cli_todos:
            if cli['tempo_total'] <> 0:
                html = '<tr >'
                html += '<td> %(cod)s </td>' % {'cod': cli['CODIGO']}
                html += '<td> %(slink)s </td>' % {'slink': cli['RAZAO'].decode('latin-1', 'ignore')}
                html += '<td> %(status)s </td>' % {'status': cli['STATUS']}
                html += '<td>%d</td>' % int(cli['qtd_pessoal'])
                html += '<td>%d</td>' % int(cli['qtd_fiscal'])
                html += '<td>%d</td>' % int(cli['qtd_contabil'])
                html += '<td>%d</td>' % int(cli['tempo_pessoal'])
                html += '<td>%d</td>' % int(cli['tempo_fiscal'])
                html += '<td>%d</td>' % int(cli['tempo_contabil'])
                html += '<td>%d</td>' % int(cli['tempo_total'])
                html += '</tr>'
                lst_html.append({'linha': html})
        print lst_html
        return lst_html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_qtdtmp(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('fb')
        sql = ''' select 
            DEPTO,USER_DOMINIO,USER_CONTROLE,COD_CONTROLE,COD_DOMINIO,QTD,TEMPO,USER_CONFERE,QTD_CONFERE,TEMPO_CONFERE,DAT_CAD
            from SERV_QTDLANC where COMPETENCIA='%(dtini)s' and IDDEPTO = %(setor)s 
            order by USER_DOMINIO
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        descrel = cursorfb.description
        rowsetfb = cursorfb.fetchall()

        lst_html = list()
        html = ''
        html += '<thead><tr>'
        html += '<th> Depto </th><th> Dominio </th><th> Controle </th><th> Cliente </th><th> Cliente Dominio </th>'
        html += '<th>QTD(min)</th><th>Tempo(min)</th>'
        html += '<th>Usuario(Confere)</th><th>QTD(Conferido)</th><th>Tempo(Conferido)</th>'
        html += '<th>Data</th></tr></thead>'
        lst_html.append({'linha': html})
        for row in rowsetfb:
            html = '<tr>'
            for col in range(len(descrel)):
                if type(row[col]) == str:
                    html = html + "<td> %s </td> " % unicode(row[col])
                elif row[col] is None:
                    html = html + "<td> &nbsp; </td> "
                elif type(row[col]) == str:
                    html = html + "<td> %s </td> " % unicode(row[col])
                elif type(row[col]) is float:
                    html = html + "<td> %s </td> " % locale.format("%.2f", row[col], grouping=True, monetary=True)
                elif descrel[col][1] is decimal.Decimal:
                    html = html + "<td align=right > %s </td> " % locale.format("%.2f", row[col], grouping=True,
                                                                                monetary=True)
                elif type(row[col]) is datetime.datetime:
                    html = html + "<td> %s </td> " % row[col].strftime("%d/%m/%Y")
                else:
                    html = html + "<td> %s </td> " % (row[col])
            html = html + '</tr>'
            lst_html.append({'linha': html})
        return lst_html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totuser3(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        numdepto = kwargs['setor']
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        lst_usr = self.lista_user2(strdataini)
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''
            
select nome , sum(total) as total from
(
select SERVICOS2.userrealiz as nome,sum(coalesce( servicos2.tempo_gasto,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join depto on depto.nomedepartamento=protocolo2.deptouser
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and depto.id_depto = %(setor)s
             and tpserv.idtpserv is not null
             and SERVICOS2.userrealiz is not null
            group by SERVICOS2.userrealiz

union all
select SERVICOS2.useraux as nome,sum(coalesce( servicos2.tempo_useraux,0)) AS TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            left join depto on depto.nomedepartamento=protocolo2.deptouser
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and depto.id_depto = %(setor)s
             and tpserv.idtpserv is not null
             and SERVICOS2.useraux is not null
            group by SERVICOS2.useraux
union all
select USUARIO.nome,sum(coalesce(serv_qtdlanc.tempo,0)) AS QTD from USUARIO
left join depto on depto.nomedepartamento=USUARIO.depto
left join serv_qtdlanc on serv_qtdlanc.user_controle=usuario.nome
            and serv_qtdlanc.iddepto= %(setor)s
            and serv_qtdlanc.competencia = '%(dtini)s'
WHERE depto.id_depto = %(setor)s
 AND USUARIO.status='N'
group by usuario.nome

union all
select USUARIO.nome,sum(coalesce(serv_qtdlanc.tempo_confere,0)) AS QTD from USUARIO
left join depto on depto.nomedepartamento=USUARIO.depto
left join serv_qtdlanc on serv_qtdlanc.user_confere=usuario.nome
            and serv_qtdlanc.iddepto= %(setor)s
            and serv_qtdlanc.competencia = '%(dtini)s'
WHERE depto.id_depto = %(setor)s
 AND USUARIO.status='N'
group by usuario.nome

union all
select SERV_OUTROS.userresp,sum(coalesce(SERV_OUTROS.tempo_gasto,0)) AS QTD from SERV_OUTROS
left join usuario on usuario.nome=serv_outros.userresp
left join depto on depto.nomedepartamento=USUARIO.depto
WHERE depto.id_depto = %(setor)s
 and serv_outros.datref =  '%(dtini)s'
group by SERV_OUTROS.userresp

) as t
group by nome
order by nome

            
            ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'setor': numdepto}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # rowsetfb = contesputil.dictcursor(cursorfb)
        # print rowsetfb
        for row in rowsetfb:
            linha = dict();
            usuarios = [item for item in lst_usr if item['NOME'] == row[0]]
            tempo = 0.0
            equipe = ''
            tempo_desc = 0
            if usuarios:
                tempo = (usuarios[0]['HRS_META'] or 0.0)
                if (datainicial.month == hj.month) and (datainicial.year == hj.year):
                    tempo_desc = 0
                else:
                    tempo_desc = (usuarios[0]['TMP_DESC'] or 0)
                equipe = (usuarios[0]['GRP_NOME'] or '')
                tmp_gasto = (row[1] or 0) / 60
            linha['nome'] = unicode(row[0])
            linha['equipe'] = unicode(equipe)
            linha['hrs_trab'] = tmp_gasto
            linha['meta'] = int(((tempo * qtddias) - tempo_desc) / 60)
            if linha['meta'] < 0:
                linha['meta'] = 0
            if linha['meta'] > 168:
                linha['meta'] = 168
            lst_serv.append(linha)
        return lst_serv

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_totdepto(self, **kwargs):
        lst_serv = list()
        strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursorufb()
        sql = '''select protocolo2.deptouser, tpserv.idtpserv, tpserv.nome,tpserv.tmp_medio , tpserv.campo_calc_temp as campo ,count(1) as qtd,sum(coalesce( servicos2.tempo_gasto,0)) AS TEMPO_TOTAL from servicos2
            inner join protocolo2 on protocolo2.idprot2=servicos2.idprot2
            left join tpserv on tpserv.idtpserv=servicos2.idservico
            where protocolo2.emissao >= '%(dtini)s'
             and protocolo2.emissao < '%(dtfim)s'
             and protocolo2.status in ('B','N')
             and tpserv.idtpserv is not null
            group by  protocolo2.deptouser, tpserv.idtpserv ,tpserv.nome,tpserv.tmp_medio, tpserv.campo_calc_temp
            order by  protocolo2.deptouser, tpserv.idtpserv ,tpserv.nome,tpserv.tmp_medio, tpserv.campo_calc_temp
         ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        # xstr = lambda s: s or ""
        linha = ''
        for row in rowsetfb:
            print row
            linha = row[0]
            linha += '</td><td>' + unicode(row[1])
            linha += '</td><td>' + unicode(row[2])
            linha += '</td><td>' + unicode(row[3])
            linha += '</td><td>' + unicode(row[4])
            linha += '</td><td>' + unicode(row[5])
            linha += '</td><td>' + unicode(row[6])
            lst_serv.append(linha)
        return lst_serv

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_qtdlancfiscal(self, **kwargs):
        lst_lanc = list()
        lst_confere = list()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT codi_usu, count(1),codi_emp
         FROM bethadba.efentradas WITH (NOLOCK)
         where dorig_ent >= '%(dtini)s' and dorig_ent <= '%(dtfim)s' group by codi_usu,codi_emp order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if (item.nome == row[0]) and (item.cliente == row[2])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.entrada = row[1]
                usuario.confere = row[1]
                usuario.cliente = row[2]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.entrada = usuario.entrada + row[1]
                usuario.confere += row[1]

        sql = '''SELECT codi_usu, count(1),codi_emp
         FROM bethadba.efsaidas WITH (NOLOCK)
         where dorig_sai >= '%(dtini)s' and dorig_sai <= '%(dtfim)s' group by codi_usu,codi_emp order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if (item.nome == row[0]) and (item.cliente == row[2])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.saida = row[1]
                usuario.cliente = row[2]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.saida = usuario.saida + row[1]

        sql = '''SELECT codi_usu, count(1),codi_emp
         FROM bethadba.efsaidas WITH (NOLOCK)
         where dorig_sai >= '%(dtini)s' and dorig_sai <= '%(dtfim)s' and codi_esp<>37 group by codi_usu,codi_emp order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if (item.nome == row[0]) and (item.cliente == row[2])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.confere = row[1]
                usuario.cliente = row[2]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.confere += row[1]

        sql = '''SELECT codi_usu, count(1),codi_emp
         FROM bethadba.efservicos WITH (NOLOCK)
         where dorig_ser >= '%(dtini)s' and dorig_ser <= '%(dtfim)s' group by codi_usu,codi_emp order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if (item.nome == row[0]) and (item.cliente == row[2])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.servico = row[1]
                usuario.confere = row[1]
                usuario.cliente = row[2]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.servico = usuario.servico + row[1]
                usuario.confere += row[1]

        # CUPON FISCAL
        sql = '''SELECT codi_emp,count(1) as qtd FROM bethadba.EFECF_REDUCAO_Z WITH (NOLOCK)
         where data_origem  >= '%(dtini)s' and data_origem  <= '%(dtfim)s' group by codi_emp order by codi_emp ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            if row[0]:
                sql2 = '''select usuario.login_dominio from grupousr_cli_vig
                        left outer join cliente on cliente.codigo=grupousr_cli_vig.idcliente
                        left outer join GRUPOUSR on GRUPOUSR.id=grupousr_cli_vig.id_grupousr_vig
                        left outer join usuario on usuario.iduser=grupousr.id_usr_responsavel
                        where cliente.cli_coddominio = %(codi_emp)s
                         and grupousr_cli_vig.vigencia='%(dtini)s'
                         and GRUPOUSR.id_depto=3
                         ''' % {'codi_emp': str(row[0]), 'dtini': strdataini}
                crusr = contesputil.ret_cursor('fb')
                crusr.execute(sql2.encode('ascii'))
                rowusr = crusr.fetchone()
                if rowusr:
                    if rowusr[0] is None:
                        usr_nome = u'None'
                    else:
                        usr_nome = rowusr[0]
                else:
                    usr_nome = u'None'
                usuarios = [item for item in lst_lanc if (item.nome == usr_nome) and (item.cliente == row[0])]
                if not usuarios:
                    usuario = qtdlanc(usr_nome)
                    usuario.cupon = row[1]
                    usuario.confere = row[1]
                    usuario.cliente = row[0]
                    lst_lanc.append(usuario)
                else:
                    usuario = usuarios[0]
                    usuario.cupon = usuario.cupon + row[1]
                    usuario.confere += row[1]

        sql = '''delete from serv_qtdlanc
            where iddepto=3 and competencia='%(comp)s' and ((confere is null) or (confere ='N' ))
             ''' % {'comp': strdataini}
        contesputil.execsql(sql, 'fb')
        d = list()
        for item in lst_lanc:
            d.append(str(item.nome) +
                     '</td><td>' + str(item.entrada) +
                     '</td><td>' + str(item.saida) +
                     '</td><td>' + str(item.servico) +
                     '</td><td>' + str(item.cupon) +
                     '</td><td>' + str(item.total()))
            sql = u'''insert into SERV_QTDLANC
                (IDDEPTO,USER_DOMINIO,COMPETENCIA,QTD,TIPO,QTD_CONFERE,COD_DOMINIO)
                values (3,'%(usdominio)s','%(comp)s', %(pqtd)s , 1 ,  %(pqtdconf)s , %(codcli)s )
                ''' % {'usdominio': unicode(item.nome), 'comp': strdataini, 'pqtd': unicode(item.total()),
                       'pqtdconf': unicode('0'), 'codcli': unicode(item.cliente)}
            contesputil.execsql(sql, 'fb')
        return d

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_qtdlancfiscal2(self, **kwargs):
        lst_lanc = list()
        if kwargs.has_key('dt_ini'):
            strdataini = kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = d + datetime.timedelta(days=1)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT codi_usu, count(1)
         FROM bethadba.efentradas WITH (NOLOCK)
         where dorig_ent >= '%(dtini)s' and dorig_ent <= '%(dtfim)s' group by codi_usu order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if item.nome == row[0]]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.entrada = row[1]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.entrada = usuario.entrada + row[1]

        sql = '''SELECT codi_usu, count(1)
         FROM bethadba.efsaidas WITH (NOLOCK)
         where dorig_sai >= '%(dtini)s' and dorig_sai <= '%(dtfim)s' group by codi_usu order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if item.nome == row[0]]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.saida = row[1]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.saida = usuario.saida + row[1]

        sql = '''SELECT codi_usu, count(1)
         FROM bethadba.efservicos WITH (NOLOCK)
         where dorig_ser >= '%(dtini)s' and dorig_ser <= '%(dtfim)s' group by codi_usu order by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if item.nome == row[0]]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.servico = row[1]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.servico = usuario.servico + row[1]

        # CUPON FISCAL
        sql = '''SELECT codi_emp,count(1) as qtd FROM bethadba.EFECF_REDUCAO_Z WITH (NOLOCK)
         where data_origem  >= '%(dtini)s' and data_origem  <= '%(dtfim)s' group by codi_emp order by codi_emp ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            if row[0]:
                sql2 = '''select usuario.login_dominio from GRUPOUSR_CLIENTE
                        left outer join cliente on cliente.codigo=grupousr_cliente.idcliente
                        left outer join GRUPOUSR on GRUPOUSR.id=GRUPOUSR_CLIENTE.id_grupousr
                        left outer join usuario on usuario.iduser=grupousr.id_usr_responsavel
                        where cliente.cli_coddominio = %s
                         and GRUPOUSR.id_depto=3
                         ''' % str(row[0])
                crusr = contesputil.ret_cursor('fb')
                crusr.execute(sql2.encode('ascii'))
                rowusr = crusr.fetchone()
                if rowusr:
                    if rowusr[0] is None:
                        usr_nome = u'None'
                    else:
                        usr_nome = rowusr[0]
                else:
                    usr_nome = u'None'
                usuarios = [item for item in lst_lanc if item.nome == usr_nome]
                if not usuarios:
                    usuario = qtdlanc(usr_nome)
                    usuario.cupon = row[1]
                    lst_lanc.append(usuario)
                else:
                    usuario = usuarios[0]
                    usuario.cupon = usuario.cupon + row[1]

        d = list()
        for item in lst_lanc:
            d.append(str(item.nome) +
                     '</td><td>' + str(item.entrada) +
                     '</td><td>' + str(item.saida) +
                     '</td><td>' + str(item.servico) +
                     '</td><td>' + str(item.cupon) +
                     '</td><td>' + str(item.total()))
        return d

    @cherrypy.expose
    def qtdconfcontabil(self, **kwargs):

        if kwargs.has_key('dt_ini'):

            ini_data = datetime.datetime.strptime(kwargs['dt_ini'], '%d/%m/%Y').date()
        else:
            ini_data = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
        html = ' %s <table>' % str(ini_data)
        # pega todos os clientes e datas alteradas no mes selecionado
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT distinct codi_emp,year(data_lan) as ano, month(data_lan) as mes 
         FROM bethadba.ctlancto WITH (NOLOCK)
         where dorig_lan = '%(dtini)s'  and codi_emp < 5000
          and orig_lan <> 5
          order by codi_emp
          ''' % {'dtini': ini_data.strftime('%Y%m%d')}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            ini_alterada = datetime.date(row[1], row[2], 1)
            fim_alterada = contesputil.add_one_month(ini_alterada)
            cli_alterado = row[0]
            # pega o total de lanc no periodo pra o cliente
            sqllimpa = ''' delete from cli_QTDLANC
            where IDDEPTO = 4 and COD_DOMINIO=%(iddominio)s
             and COMPETENCIA = '%(comp)s'
            ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y")}
            contesputil.execsql(sqllimpa, 'fb')
            cr_alterado = contesputil.ret_cursor('do')
            sqlalt = '''SELECT count(1) as qtd ,upper( codi_usu)
             FROM bethadba.ctlancto WITH (NOLOCK)
             where data_lan >= '%(dtini)s' and data_lan < '%(dtfim)s'
              and codi_emp = '%(cli)s'
                  and orig_lan <> 5

              group by codi_usu
              ''' % {'dtini': ini_alterada.strftime("%Y%m%d"), 'dtfim': fim_alterada.strftime("%Y%m%d"),
                     'cli': str(cli_alterado)}
            cr_alterado.execute(sqlalt.encode('ascii'))
            rs_alterado = cr_alterado.fetchall()
            for comp_alt in rs_alterado:
                sql2 = '''insert into cli_QTDLANC
                (IDDEPTO,COD_DOMINIO,COMPETENCIA,QTD,usuario )
                values (4,'%(iddominio)s','%(comp)s', %(pqtd)s,'%(usua)s' )
                ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                       'pqtd': str(comp_alt[0]), 'usua': unicode(comp_alt[1])}
                contesputil.execsql(sql2, 'fb')
                html += '<tr><td>%(iddominio)s</td><td>%(comp)s</td><td>%(pqtd)s</td> </tr>' % {
                    'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                    'pqtd': unicode(comp_alt[0])}
        html += '</table>'
        return html

    @cherrypy.expose
    def qtdconflstcontabil(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            ini_data = datetime.datetime.strptime(kwargs['dt_ini'], '%d/%m/%Y').date()
        else:
            ini_data = datetime.date.today()
        cemp = 0
        if kwargs.has_key('idemp'):
            cemp = int(kwargs['idemp'])
        html = ''
        if cemp > 0:
            sql = '''
            SELECT top 100 *
             FROM bethadba.ctlancto WITH (NOLOCK)
             where codi_emp=%(iddominio)s  and codi_emp < 5000
              and data_lan >= '%(comp)s'
              order by data_lan
            ''' % {'iddominio': str(cemp), 'comp': ini_data.strftime("%Y%m%d")}
            cr = contesputil.ret_cursor('do')
            cr.execute(sql.encode('ascii'))
            rs = cr.fetchall()
            descr = cr.description
            html = html + '<tr>'
            for col in range(len(descr)):
                html = html + "<td> %s </td>" % (descr[col][0])
            html = html + '</tr>'

            for row in rs:
                html = html + '<tr>'
                for col in range(len(descr)):
                    if type(row[col]) == str:
                        html = html + "<td> %s </td> " % row[col].decode('latin-1')
                    elif row[col] is None:
                        html = html + "<td> &nbsp; </td> "
                    elif type(row[col]) == str:
                        html = html + "<td> %s </td> " % row[col].decode('latin-1')
                    elif type(row[col]) is float:
                        html = html + "<td> %s </td> " % locale.format("%.2f", row[col], grouping=True, monetary=True)
                    elif descr[col][1] is decimal.Decimal:
                        html = html + "<td align=right > %s </td> " % locale.format("%.2f", row[col], grouping=True,
                                                                                    monetary=True)
                    elif type(row[col]) is datetime.datetime:
                        html = html + "<td> %s </td> " % row[col].strftime("%d/%m/%Y")
                    else:
                        html = html + "<td> %s </td> " % (row[col])
                html = html + '</tr>'

        dtmpl = dict()
        dtmpl['scompt'] = ini_data.strftime("%d/%m/%Y")
        dtmpl['sidemp'] = str(cemp)
        dtmpl['session'] = cherrypy.session
        dtmpl['tbl_lista'] = html
        tmpl = env.get_template('lstconfcont.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def qtdconfmescontabil(self, **kwargs):

        if kwargs.has_key('dt_ini'):

            ini_data = datetime.datetime.strptime(kwargs['dt_ini'], '%d/%m/%Y').date()
        else:
            ini_data = datetime.date.fromordinal(datetime.date.today().toordinal() - 30)
        html = ' %s <table>' % str(ini_data)
        for l in range(30):
            # pega todos os clientes e datas alteradas no mes selecionado
            cursorfb = contesputil.ret_cursor('do')
            sql = '''SELECT distinct codi_emp,year(data_lan) as ano, month(data_lan) as mes 
             FROM bethadba.ctlancto WITH (NOLOCK)
             where dorig_lan = '%(dtini)s'  and codi_emp < 5000
              order by codi_emp
              ''' % {'dtini': ini_data.strftime('%Y%m%d')}
            cursorfb.execute(sql.encode('ascii'))
            rowsetfb = cursorfb.fetchall()

            for row in rowsetfb:
                ini_alterada = datetime.date(row[1], row[2], 1)
                fim_alterada = contesputil.add_one_month(ini_alterada)
                cli_alterado = row[0]
                # pega o total de lanc no periodo pra o cliente
                sqllimpa = ''' delete from cli_QTDLANC
                where IDDEPTO = 4 and COD_DOMINIO=%(iddominio)s
                 and COMPETENCIA = '%(comp)s'
                ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y")}
                contesputil.execsql(sqllimpa, 'fb')
                cr_alterado = contesputil.ret_cursor('do')
                sqlalt = '''SELECT count(1) as qtd ,upper( codi_usu)
                 FROM bethadba.ctlancto WITH (NOLOCK)
                 where data_lan >= '%(dtini)s' and data_lan < '%(dtfim)s'
                  and codi_emp = '%(cli)s'
                          and orig_lan <> 5
                  group by codi_usu
                  ''' % {'dtini': ini_alterada.strftime("%Y%m%d"), 'dtfim': fim_alterada.strftime("%Y%m%d"),
                         'cli': str(cli_alterado)}
                cr_alterado.execute(sqlalt.encode('ascii'))
                rs_alterado = cr_alterado.fetchall()
                for comp_alt in rs_alterado:
                    sql2 = '''insert into cli_QTDLANC
                    (IDDEPTO,COD_DOMINIO,COMPETENCIA,QTD,usuario )
                    values (4,'%(iddominio)s','%(comp)s', %(pqtd)s,'%(usua)s' )
                    ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                           'pqtd': str(comp_alt[0]), 'usua': str(comp_alt[1])}
                    contesputil.execsql(sql2, 'fb')
                    htmltext = '<tr><td>%(iddominio)s</td><td>%(comp)s</td><td>%(pqtd)s</td> </tr>' % {
                        'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                        'pqtd': unicode(comp_alt[0])}
                    print htmltext
                    html += htmltext
            ini_data = datetime.date.fromordinal(ini_data.toordinal() + 1)
        html += '</table>'
        return html

    @cherrypy.expose
    def qtdconfdaycontabil(self, **kwargs):

        if kwargs.has_key('dt_ini'):
            ini_data = datetime.datetime.strptime(kwargs['dt_ini'], '%d/%m/%Y').date()
        else:
            ini_data = datetime.date.fromordinal(datetime.date.today().toordinal())
        html = ' %s <table>' % str(ini_data)
        # pega todos os clientes e datas alteradas no mes selecionado
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT distinct codi_emp,year(data_lan) as ano, month(data_lan) as mes 
         FROM bethadba.ctlancto WITH (NOLOCK)
         where data_lan = '%(dtini)s'  and codi_emp < 5000
          order by codi_emp
          ''' % {'dtini': ini_data.strftime('%Y%m%d')}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            ini_alterada = datetime.date(row[1], row[2], 1)
            fim_alterada = contesputil.add_one_month(ini_alterada)
            cli_alterado = row[0]
            # pega o total de lanc no periodo pra o cliente
            sqllimpa = ''' delete from cli_QTDLANC
            where IDDEPTO = 4 and COD_DOMINIO=%(iddominio)s
             and COMPETENCIA = '%(comp)s'
            ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y")}
            contesputil.execsql(sqllimpa, 'fb')
            cr_alterado = contesputil.ret_cursor('do')
            sqlalt = '''SELECT count(1) as qtd ,upper( codi_usu)
             FROM bethadba.ctlancto WITH (NOLOCK)
             where data_lan >= '%(dtini)s' and data_lan < '%(dtfim)s'
              and codi_emp = '%(cli)s'
                  and orig_lan <> 5
              group by codi_usu
              ''' % {'dtini': ini_alterada.strftime("%Y%m%d"), 'dtfim': fim_alterada.strftime("%Y%m%d"),
                     'cli': str(cli_alterado)}
            cr_alterado.execute(sqlalt.encode('ascii'))
            rs_alterado = cr_alterado.fetchall()
            for comp_alt in rs_alterado:
                sql2 = '''insert into cli_QTDLANC
                (IDDEPTO,COD_DOMINIO,COMPETENCIA,QTD,usuario )
                values (4,'%(iddominio)s','%(comp)s', %(pqtd)s,'%(usua)s' )
                ''' % {'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                       'pqtd': str(comp_alt[0]), 'usua': str(comp_alt[1])}
                contesputil.execsql(sql2, 'fb')
                htmltxt = '<tr><td>%(iddominio)s</td><td>%(comp)s</td><td>%(pqtd)s</td> </tr>' % {
                    'iddominio': str(cli_alterado), 'comp': ini_alterada.strftime("%d.%m.%Y"),
                    'pqtd': unicode(comp_alt[0])}
                print htmltxt
                html += htmltxt
        html += '</table>'
        return html

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_qtdlanccontabil(self, **kwargs):
        lst_lanc = list()
        lst_lcli = list()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT upper(codi_usu), count(1),codi_emp,orig_lan, cast(dateformat( data_lan,'YYYYMM01') as date) AS COMPTDOC
         FROM bethadba.ctlancto WITH (NOLOCK)
         where dorig_lan >= '%(dtini)s' and dorig_lan <= '%(dtfim)s' 
          group by codi_usu , codi_emp,orig_lan, cast(dateformat( data_lan,'YYYYMM01') as date)  ORDER by codi_usu ''' % {
            'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if
                        (item.nome == row[0]) and (item.cliente == row[2]) and (usuario.orig_lan == row[3]) and (
                                    usuario.comptdoc == row[4])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.entrada = row[1]
                usuario.cliente = row[2]
                usuario.orig_lan = row[3]
                usuario.comptdoc = row[4]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.entrada = usuario.entrada + row[1]

        d = list()
        sql = '''delete from serv_qtdlanc
            where iddepto=4 and competencia='%(comp)s'
             ''' % {'comp': strdataini}
        contesputil.execsql(sql, 'fb')
        for item in lst_lanc:
            d.append(str(item.nome) +
                     '</td><td>' + str(item.total()))
            sql = u'''insert into SERV_QTDLANC
                (IDDEPTO,USER_DOMINIO,COMPETENCIA,QTD,COD_DOMINIO,tipo,COMPT_DOC)
                values (4,'%(usdominio)s','%(comp)s', %(pqtd)s , %(codcli)s , %(tipo)s , '%(comptdoc)s' )
                ''' % {'usdominio': unicode(item.nome), 'comp': strdataini, 'pqtd': unicode(item.total()),
                       'codcli': unicode(item.cliente), 'tipo': unicode(item.orig_lan),
                       'comptdoc': item.comptdoc.strftime('%d.%m.%Y')}
            print sql
            contesputil.execsql(sql, 'fb')

        # Insere qtd Pessoal
        sql = '''delete from serv_qtdlanc
            where iddepto=2 and competencia='%(comp)s'
             ''' % {'comp': strdataini}
        contesputil.execsql(sql, 'fb')
        cursorfb = contesputil.ret_cursor('do')
        sql = '''
            SELECT bas.codi_emp, count(1) as total_geral                   
                FROM bethadba.fobasesserv AS bas,                
                bethadba.foempregados AS fun           
            WHERE bas.codi_emp = fun.codi_emp AND            
                fun.i_empregados = bas.i_empregados AND     
                bas.competencia = '%(dtini)s'    and        
                bas.rateio = 0 and
                bas.tipo_process=11     
            group by bas.codi_emp      ''' % {'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            sql = u'''insert into SERV_QTDLANC
                (IDDEPTO,USER_DOMINIO,COMPETENCIA,QTD,COD_DOMINIO)
                values (2,'','%(comp)s', %(pqtd)s , %(codcli)s )
                ''' % {'comp': strdataini, 'pqtd': unicode(row[1]), 'codcli': unicode(row[0])}
            contesputil.execsql(sql, 'fb')

        return d

    @cherrypy.expose
    def cliente_email(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        dtmpl = dict()
        dtmpl['session'] = cherrypy.session
        dtmpl['sel_cli'] = self.getcombocli(selcli, 'N')
        tmpl = env.get_template('cliente_email.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def rpaimp(self, **kwargs):
        codempr = kwargs.get('codempr', '0')
        codacum = kwargs.get('codacum', '0')
        txt = kwargs.get('selarq', None)
        print (type(txt))
        imp = None
        try:
            txtarq = txt.file
        except:
            txtarq = None

        print(kwargs)
        print (type(txtarq))
        size = 0
        if txtarq:
            import rpaimport
            rpa = rpaimport.formrpa(txt,codempr,codacum)
            imp = rpa.importar(txtarq)

        dtmpl = dict()
        dtmpl['session'] = cherrypy.session
        dtmpl['codempr'] = codempr
        dtmpl['codacum'] = codacum
        dtmpl['retmsg'] = imp
        tmpl = env.get_template('rpaimp.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    def rpaimpup(self, **kwargs):
        print(kwargs)
        codempr = kwargs.get('codempr', '0')
        codacum = kwargs.get('codacum', '0')
        txt = kwargs.get('selarq', None)
        print (type(txt))
        txtarq = txt.file


        print (type(txtarq))
        size = 0
        if txtarq:
            while True:
                data = txtarq.read(8192)
                print (data)
                if not data:
                    break
                size += len(data)
        dtmpl = dict()
        dtmpl['session'] = cherrypy.session
        dtmpl['codempr'] = codempr
        dtmpl['codacum'] = codacum


        return 'FOI'


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_email(self, **kwargs):
        str_emails = ''
        lst_emails = []

        sql = ''' SELECT EMAIL,razao,codigo from cliente where 1=1    '''
        if kwargs.has_key('ativos'):
            sql += " and status = 'A' and  TERM_ATIV is null "
        if kwargs.has_key('resp_financ'):
            sql += " and RESP_FINANCEIRO = 'S' "
        if kwargs.has_key('ratividade'):
            sql += " and RAMATIV = '" + kwargs.get('ratividade', '').encode('latin-1') + "' "
        sql += " order by razao "
        #cursorfb = contesputil.ret_cursor('fb')
        import fdb
        conn = fdb.connect(
            host='192.168.0.20',
            database='/home/bd2/Bco_dados/CONTESP.GDB',
            user='sysdba',
            password='masterkey',
            charset='UTF8'
        )
        cursorfb = conn.cursor()
        cursorfb.execute(sql)
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            lst_emails.append(
                {'razao': row[1], 'email': row[0], 'codigo': row[2]})
            if row[0]:
                str_emails += row[0] + ';'
        return {'email': str_emails, 'clientes': lst_emails}

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_email_atualiza(self, **kwargs):
        print kwargs
        selcli = kwargs.get('codcli', '0')
        novoemail = kwargs.get('email', '')
        seltipo = kwargs.get('tipo', '0')
        campo_atera = ''
        if seltipo == '1':
            campo_atera = 'EMAIL'
        if seltipo == '2':
            campo_atera = 'EMAILPESSOAL'
        if seltipo == '3':
            campo_atera = 'EMAILFISCAL'
        if seltipo == '4':
            campo_atera = 'EMAILCONTABIL'
        str_emails = ''
        sql = ''' UPDATE cliente SET  %(campo)s =  '%(email)s'
        WHERE CODIGO =  %(codcli)s  ''' % {'codcli': selcli, 'campo': campo_atera, 'email': novoemail}
        print sql
        contesputil.execsql(sql, 'fb')
        return {'campo_alterado': campo_atera}

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_email_dados(self, **kwargs):
        selcli = kwargs.get('codcli', '0')
        sql = ''' SELECT codigo,razao,email,TEL01,TEL02,TEL03,TEL04,
         DESCRTEL1,DESCRTEL2,DESCRTEL3,DESCRTEL4,
         TEL01CONTATO,TEL02CONTATO,TEL03CONTATO,TEL04CONTATO,
         EMAILPESSOAL,EMAILFISCAL,EMAILCONTABIL,EMAILFINANCEIRO
         from cliente where  CODIGO = %(codcli)s   ''' % {'codcli': selcli}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        dados_cliente = contesputil.dictcursor2(cursorfb)
        if dados_cliente:
            dados_cliente = dados_cliente[0]
        return {'dados_cliente': dados_cliente}

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_responsavelcb(self, **kwargs):
        selcli = kwargs.get('codcli', '0')
        sql = ''' SELECT CODCLI,IDCLIRESP,NOMRESP
         from CLIRESPONSAVEL where  CODCLI = %(codcli)s
          order by NOMRESP ''' % {'codcli': selcli}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        dados_resp = contesputil.dictcursor2(cursorfb)
        return dados_resp

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_tributacao(self, **kwargs):
        selcli = kwargs.get('codcli', '0')
        sql = '''SELECT top 1 bethadba.EFPARAMETRO_VIGENCIA.RFED_PAR as enquadramento FROM bethadba.EFPARAMETRO_VIGENCIA
         WHERE bethadba.EFPARAMETRO_VIGENCIA.CODI_EMP = %(codempr)s
        AND bethadba.EFPARAMETRO_VIGENCIA.VIGENCIA_PAR<= '%(dtatual)s'
        ORDER BY bethadba.EFPARAMETRO_VIGENCIA.VIGENCIA_PAR DESC
        ''' % {'codempr': selcli, 'dtatual': datetime.datetime.now().strftime("%Y%m%d")}
        print sql
        cursorfb = contesputil.ret_cursor('do')
        cursorfb.execute(sql.encode('ascii'))
        dados_resp = contesputil.dictcursor2(cursorfb)
        return dados_resp

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_nfsaida_json(self, **kwargs):
        print kwargs
        selcli = kwargs.get('codcli', '0')
        dtdoc = kwargs.get('dtdoc', '')
        nnf = kwargs.get('nnf', '0')
        sql = '''SELECT bethadba.efsaidas.codi_emp, bethadba.efsaidas.codi_sai, bethadba.efsaidas.codi_acu
        , bethadba.efsaidas.codi_nat, bethadba.efsaidas.nume_sai,bethadba.efsaidas.vcon_sai, bethadba.EFACUMULADOR.NOME_ACU
           FROM bethadba.efsaidas
           left join bethadba.EFACUMULADOR on bethadba.EFACUMULADOR.codi_emp=bethadba.efsaidas.codi_emp
             and bethadba.efsaidas.codi_acu=bethadba.EFACUMULADOR.codi_acu
        where bethadba.efsaidas.nume_sai = '%(nnf)s'
        and bethadba.efsaidas.dorig_sai >= '%(dtdoc)s'
        and bethadba.efsaidas.codi_emp=%(codempr)s
        ''' % {'codempr': selcli, 'dtdoc': dtdoc, 'nnf': nnf}
        print sql
        cursorfb = contesputil.ret_cursor('do')
        cursorfb.execute(sql.encode('ascii'))
        dadosreg = contesputil.dictcursor2(cursorfb)
        return dadosreg

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_cliente_responsaveltelcb(self, **kwargs):
        selresp = kwargs.get('codresp', '0')
        dados_resp = None
        if int(selresp) >0:
            sql = ''' SELECT * from CLIRESPTEL where  CODRESP = %(codresp)s
              order by TIPO,DESCRTELR ''' % {'codresp': selresp}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql)
            dados_resp = contesputil.dictcursor2(cursorfb)
        return dados_resp

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def digitacao_fiscal_json(self, **kwargs):
        print kwargs
        pid = kwargs.get('pid', '0')

        sql = ''' select DIGITACAO_VIG.competencia,DIGITACAO_VIG.tempo,DIGITACAO_VIG.DTCAD,DIGITACAO_VIG.DTINI,DIGITACAO_VIG.DTFIM,
                    DIGITACAO_VIG.ID,DIGITACAO_VIG.CODCLI,DIGITACAO_VIG.IDTPSERVICO,tpserv.nome as SERVICO,DIGITACAO_VIG.USUARIO,DIGITACAO_VIG.DTLANC,DIGITACAO_VIG.STATUS,
                    cliente.razao from DIGITACAO_VIG                    
                left join cliente on cliente.codigo=DIGITACAO_VIG.codcli
                left join tpserv on tpserv.IDTPSERV=DIGITACAO_VIG.IDTPSERVICO                    
                where DIGITACAO_VIG.ID = %(ID)s
                order by DIGITACAO_VIG.ID ''' % {'ID': pid}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql.encode('ascii'))
        dadoslis = contesputil.dictcursor2(cursorfb)
        dadosreg = dict()
        if dadoslis:
            dadosreg = dadoslis[0]
        return dadosreg


    @cherrypy.expose
    def digitacao_fiscal(self, **kwargs):
        gerar_rel = 0
        import pprint
        pprint.pprint(kwargs)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            gerar_rel += 1
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        if kwargs.has_key('dt_fim'):
            gerar_rel += 1
            strdatafim = '01.' + kwargs['dt_fim'].replace('/', '.')
        else:
            strdatafim = contesputil.add_one_month(datainicial).strftime('%d.%m.%Y')
        selcomptfim = strdatafim[3:].replace('.', '/')
        datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        tpfiltro = 'DIGITACAO_VIG.competencia'
        idtpfiltro = '0'
        if cherrypy.request.method == 'POST':
            print 'POST'
            if kwargs.has_key('ID'):
                print 'ID', kwargs['ID']
                import modelbase
                dadosvlr = kwargs
                dadosvlr['COMPETENCIA'] = dadosvlr['COMPETENCIA'].replace('/', '.')
                modelbase.sql_update('DIGITACAO_VIG', 'ID', dadosvlr, 'STATUS,TEMPO,COMPETENCIA')

        if kwargs.has_key('idtpfiltro'):
            idtpfiltro = kwargs['idtpfiltro']
            if idtpfiltro == '1':
                tpfiltro = 'DIGITACAO_VIG.DTCAD'
                strdatafim = contesputil.mkLastOfMonth(datafinal).strftime('%d.%m.%Y')
                datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        lst_dig = list()
        lst_resumo = list()
        if gerar_rel > 1:

            sql = ''' select DIGITACAO_VIG.ID, DIGITACAO_VIG.competencia,DIGITACAO_VIG.tempo,DIGITACAO_VIG.DTCAD,DIGITACAO_VIG.DTINI,DIGITACAO_VIG.DTFIM,
                        DIGITACAO_VIG.ID,DIGITACAO_VIG.CODCLI,DIGITACAO_VIG.IDTPSERVICO,tpserv.nome as SERVICO,DIGITACAO_VIG.USUARIO,DIGITACAO_VIG.DTLANC,DIGITACAO_VIG.STATUS,
                        cliente.razao from DIGITACAO_VIG                    
                    left join cliente on cliente.codigo=DIGITACAO_VIG.codcli
                    left join tpserv on tpserv.IDTPSERV=DIGITACAO_VIG.IDTPSERVICO                    
                    where %(filtro)s >= '%(dtini)s'
                     and %(filtro)s <= '%(dtfim)s'
                    order by %(filtro)s ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'filtro': tpfiltro}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql)
            lst_dig = contesputil.dictcursor3(cursorfb)
            for dig in lst_dig:
                if dig['STATUS'] == 'F':
                    dig['NM_STATUS'] = 'Finalizado'
                else:
                    dig['NM_STATUS'] = 'Executando'
                if dig['TEMPO']:
                    dig['TEMPO_FORMAT'] = '{:02d}:{:02d}'.format(*divmod(dig['TEMPO'], 60))
                else:
                    dig['TEMPO_FORMAT'] = '00:00'

        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['scomptfim'] = selcomptfim
        dtmpl['lst_dig'] = lst_dig
        dtmpl['lst_resumo'] = lst_resumo
        dtmpl['selfiltro'] = idtpfiltro
        dtmpl['session'] = cherrypy.session
        dtmpl['selstatus'] = [('F', 'Finalizado'), ('E', 'Executando')]
        dtmpl['form'] = dict()
        tmpl = env.get_template('digitacao_fiscal.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    def digitacao_contabil(self, **kwargs):
        gerar_rel = 0
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
            gerar_rel += 1
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        if kwargs.has_key('dt_fim'):
            gerar_rel += 1
            strdatafim = '01.' + kwargs['dt_fim'].replace('/', '.')
        else:
            strdatafim = contesputil.add_one_month(datainicial).strftime('%d.%m.%Y')
        selcomptfim = strdatafim[3:].replace('.', '/')
        datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        tpfiltro = 'digitacao_compt.competencia'
        idtpfiltro = '0'
        if kwargs.has_key('idtpfiltro'):
            idtpfiltro = kwargs['idtpfiltro']
            if idtpfiltro == '1':
                tpfiltro = 'digitacao.dtlanc'
                strdatafim = contesputil.mkLastOfMonth(datafinal).strftime('%d.%m.%Y')
                datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        lst_dig = list()
        lst_resumo = list()
        if gerar_rel > 1:

            sql = ''' select digitacao_compt.competencia,digitacao_compt.tempo,
                        digitacao.ID,digitacao.CODCLI,digitacao.SERVICO,digitacao.USUARIO,digitacao.DTLANC,digitacao.STATUS,
                        cliente.razao from digitacao
                    inner join digitacao_compt on digitacao_compt.id_dig=digitacao.id
                    left join cliente on cliente.codigo=digitacao.codcli
                    where %(filtro)s >= '%(dtini)s'
                     and %(filtro)s <= '%(dtfim)s'
                    order by %(filtro)s ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'filtro': tpfiltro}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql)
            lst_dig = contesputil.dictcursor3(cursorfb)
            print sql
            for dig in lst_dig:
                if dig['STATUS'] == 'F':
                    dig['NM_STATUS'] = 'Finalizado'
                else:
                    dig['NM_STATUS'] = 'Executando'
                if dig['TEMPO']:
                    dig['TEMPO_FORMAT'] = '{:02d}:{:02d}'.format(*divmod(dig['TEMPO'], 60))
                else:
                    dig['TEMPO_FORMAT'] = '00:00'

        sql = ''' select digitacao.codcli,cliente.razao,digitacao.servico,digitacao.usuario, sum(digitacao_compt.tempo) as tempo from digitacao
                inner join digitacao_compt on digitacao_compt.id_dig=digitacao.id
                left join cliente on cliente.codigo=digitacao.codcli
                where %(filtro)s >= '%(dtini)s'
                 and %(filtro)s <= '%(dtfim)s'
                group by digitacao.codcli,cliente.razao,digitacao.servico,digitacao.usuario
                order by cliente.razao ''' % {'dtini': strdataini, 'dtfim': strdatafim, 'filtro': tpfiltro}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_resumo = contesputil.dictcursor3(cursorfb)
        for dig in lst_resumo:
            if dig['TEMPO']:
                dig['TEMPO_FORMAT'] = '{:02d}:{:02d}'.format(*divmod(dig['TEMPO'], 60))
            else:
                dig['TEMPO_FORMAT'] = '00:00'

        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['scomptfim'] = selcomptfim
        dtmpl['lst_dig'] = lst_dig
        dtmpl['lst_resumo'] = lst_resumo
        dtmpl['selfiltro'] = idtpfiltro
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('digitacao_contabil.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def particularcli(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        selsetor = '0'
        if kwargs.has_key('IDSETOR'):
            selsetor = kwargs['IDSETOR']
            if selsetor == '1' and cherrypy.session.get('NIVEL', 0) < 8:
                selsetor = '0'
        dtmpl = dict()
        dtmpl['sel_cli'] = sql_param.get_sel_clientestr(selcli)
        dtmpl['session'] = cherrypy.session
        dtmpl['seldepto'] = sql_param.get_sel_depto(False)
        dtmpl['iddepto'] = selsetor
        dtmpl['tbl_arq'] = sql_param.get_particular_cli(selcli, selsetor)
        tmpl = env.get_template('particularcli.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def verifprot(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(datainicial)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')

        dtmpl = dict()
        #dtmpl['sel_cli'] = sql_param.get_sel_clientestr(selcli)
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        dtmpl['tbl_arq'] = sql_param.get_verifprot(strdataini, strdatafim)
        tmpl = env.get_template('verifprot.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    def relchamadosos(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini.replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        if kwargs.has_key('dt_fim'):
            strdatafim = kwargs['dt_fim'].replace('/', '.')
        else:
            strdatafim = contesputil.add_one_month(datainicial).strftime('%d.%m.%Y')
        selcomptfim = strdatafim.replace('.', '/')
        datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        #
        #  PEGA A LISTAGEM DE CHAMADOS
        #
        sql = ''' select CHAMADO_OS.*,tp_chamado_os.descricao FROM CHAMADO_OS
                left join tp_chamado_os  on tp_chamado_os.id=chamado_os.tpchamado_os
                where CAST(chamado_os.dat_cad AS DATE) >= '%(dtini)s'
                 and CAST(chamado_os.dat_cad AS DATE) <= '%(dtfim)s'
                order by chamado_os.dat_cad ''' % {'dtini': strdataini, 'dtfim': strdatafim}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_dig = contesputil.dictcursor3(cursorfb)

        # FIM SQL
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['scomptfim'] = selcomptfim
        dtmpl['lsos'] = lst_dig
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('relchamadosos.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def osatendjson(self, **kwargs):

        print kwargs
        codi_atend = kwargs.get('codi_atend', u'0')
        txtobs = kwargs.get('passdig', u'')
        dicl = {'confirmado': 0}
        if ((txtobs == u'13') or (txtobs == u'14') or (txtobs == u'15') or (txtobs == u'17')):
            if int(codi_atend) > 0:
                nvstatus = ''
                print 'nvstatus', nvstatus
                if txtobs == u'13':
                    nvstatus = 'S'
                if txtobs == u'14':
                    nvstatus = 'C'
                if txtobs == u'15':
                    nvstatus = 'N'
                if nvstatus != '':
                    inssql = '''
                    update CHAMADO_OS  set EMATENDIMENTO = '%(nstatus)s' where ID = %(id)s
                    ''' % {'id': codi_atend, 'nstatus': nvstatus}
                    contesputil.execsql(inssql, 'fb')
                    dicl['confirmado'] = 1
                else:
                    if txtobs == u'17':
                        nvstatus = 'S'
                        inssql = '''
                        update CHAMADO_OS  set status = '%(nstatus)s', USERBAIXA=USERCAD, DAT_BAIXA=current_timestamp where ID = %(id)s
                        ''' % {'id': codi_atend, 'nstatus': nvstatus}
                        contesputil.execsql(inssql, 'fb')
                        dicl['confirmado'] = 1

            else:
                dicl['confirmado'] = -1
        else:
            dicl['confirmado'] = -2
        return dicl

    @cherrypy.expose
    def osatend(self, **kwargs):
        if kwargs.has_key('idos'):
            Id_os = kwargs['idos']
        else:
            Id_os = ''
        if kwargs.has_key('cidos'):
            Senha_os = kwargs['cidos']
        else:
            Senha_os = ''

        msg = ''
        if (Id_os != '') and (Senha_os != ''):
            msg = 'Alterado'

        #
        #  PEGA A LISTAGEM DE CHAMADOS
        #
        sql = ''' select CHAMADO_OS.*,tp_chamado_os.descricao,usuario.depto,usuario.ramal FROM CHAMADO_OS
                left join tp_chamado_os  on tp_chamado_os.id=chamado_os.tpchamado_os
                left join usuario on usuario.nome=chamado_os.usercad
                where CHAMADO_OS.status='A'
                order by chamado_os.dat_cad '''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_dig = contesputil.dictcursor3(cursorfb)

        # FIM SQL

        dtmpl = dict()
        dtmpl['lsos'] = lst_dig
        dtmpl['updatemsg'] = msg
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('osatend.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def sgqod03(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = '01.01.' + kwargs['dt_ini']
        else:
            ini_mes = datetime.date.today().replace(day=1, month=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = datetime.date(datainicial.year + 1, 1, 1)
        selcompt = str(datainicial.year)
        strdatafim = datetime.datetime.strftime(datafinal, '%d.%m.%Y')
        reldados = dict()
        reldados['NOVOS_CLI'] = sql_param.get_clientes_novos(selcompt)
        reldados['SATISF_CLI'] = sql_param.get_satisfacao_cliente(selcompt)
        print reldados['SATISF_CLI']
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['reldados'] = reldados
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('sgqod03.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def receb_contesp(selfself, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['tabrel'] = sql_param.get_receb_contesp(selcompt)
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('receb_contesp.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def lstcliper(self, **kwargs):
        if kwargs.has_key('dt_ini'):
            strdataini = kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdataini = ini_mes.strftime('%d.%m.%Y')
        if kwargs.has_key('dt_fim'):
            strdatafim = kwargs['dt_fim'].replace('/', '.')
        else:
            fim_mes = contesputil.subtract_one_month(datetime.date.today().replace(day=1))
            strdatafim = fim_mes.strftime('%d.%m.%Y')
        selclitemp = 'N'
        selclicont = 'N'
        checkst = lambda x: 'checked' if x == 'S' else ''
        if kwargs.has_key('chkclitemp'):
            selclitemp = 'S'
        if kwargs.has_key('chkclicont'):
            selclicont = 'S'
        selcompt = strdataini.replace('.', '/')
        selcomptfim = strdatafim.replace('.', '/')
        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['scompt_fim'] = selcomptfim
        dtmpl['chkclitempst'] = checkst(selclitemp)
        dtmpl['chkclicontst'] = checkst(selclicont)
        dtmpl['tabrel'] = sql_param.get_lstcliper(selcompt, selcomptfim, selclitemp, selclicont)
        dtmpl['session'] = cherrypy.session
        dtmpl['msgresumo'] = list()
        qtdcli = 0
        qtdativ = 0
        qtdinativ = 0
        qtdtmp = 0
        qtdclicont = 0
        qtdclifin = 0
        qtdresf = 0
        for cli in dtmpl['tabrel']:
            qtdcli += 1
            if cli['STATUS'] == 'T':
                qtdtmp += 1
            if cli['STATUS'] == 'A':
                qtdativ += 1
            if cli['STATUS'] == 'I':
                qtdinativ += 1
            if cli['CLIENTE_CONTESP'] == 'S':
                qtdclicont += 1
            if cli['RESP_FINANCEIRO'] == 'S':
                qtdresf += 1
            if cli['TERM_ATIV']:
                qtdclifin += 1
        dtmpl['msgresumo'].append('Qtd clientes ' + str(qtdcli))
        dtmpl['msgresumo'].append('Qtd clientes Ativos ' + str(qtdativ))
        dtmpl['msgresumo'].append('Qtd clientes Inativos ' + str(qtdinativ))
        dtmpl['msgresumo'].append(u'Qtd clientes Temporários ' + str(qtdtmp))
        dtmpl['msgresumo'].append('Qtd clientes Contesp ' + str(qtdclicont))
        dtmpl['msgresumo'].append('Qtd clientes Finalizados ' + str(qtdclifin))
        dtmpl['msgresumo'].append('Qtd Resp. Financeiro ' + str(qtdresf))
        tmpl = env.get_template('lstcliper.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def leg_senhas_lst(self, **kwargs):
        dtmpl2 = dict()
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(kwargs)
        if kwargs.has_key('ID'):
            fid = int(kwargs['ID']) if kwargs['ID'] != '' else 0
            print 'Form'
            print fid
            campos = kwargs.copy()
            if fid == 0:
                del campos['ID']
                contesputil.add_row('fb', 'LEG_SENHAS', campos)
            else:
                contesputil.update_row('fb', 'LEG_SENHAS', 'ID', campos)
        tmpl = env.get_template('leg_senhas.html')
        dtmpl2['session'] = cherrypy.session
        dtmpl2['form'] = dict()
        dtmpl2['erros'] = dict()
        dtmpl2['SETOR_ATUAL'] = cherrypy.session.get('SETOR', '')
        filtracli = 'N' if cherrypy.session.get('NIVEL',0) < 9 else 'S'
        dtmpl2['dados'] = sql_param.get_leg_senha(filtracli)
        return tmpl.render(dtmpl2)

    @cherrypy.expose
    def leg_licencas_lst(self, **kwargs):
        dtmpl2 = dict()

        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(kwargs)
        if kwargs.has_key('ID'):
            fid = int(kwargs['ID']) if kwargs['ID'] != '' else 0
            print 'Form'
            print fid
            campos = kwargs.copy()
            keys = campos.keys()
            if 'DTEMISSAO' in keys:
                campos['DTEMISSAO'] = campos['DTEMISSAO'].replace('/', '.')
            if 'DTVENC' in keys:
                campos['DTVENC'] = campos['DTVENC'].replace('/', '.')

            if campos['DTVENC'] == u'':
                del campos['DTVENC']

            if campos['DTEMISSAO'] == u'':
                del campos['DTEMISSAO']

            if fid == 0:
                del campos['ID']
                contesputil.add_row('fb', 'LICENCAEMPRESA', campos)
            else:
                contesputil.update_row('fb', 'LICENCAEMPRESA', 'ID', campos)
        tmpl = env.get_template('leg_licencas.html')
        dtmpl2['session'] = cherrypy.session
        dtmpl2['sel_cli'] = self.getcombocli(0,'N')
        dtmpl2['form'] = dict()
        dtmpl2['erros'] = dict()
        dtmpl2['SETOR_ATUAL'] = cherrypy.session.get('SETOR', '')
        filtracli = 'N' if cherrypy.session.get('NIVEL',0) < 9 else 'S'
        dtmpl2['dados'] = sql_param.get_licenca_empr()
        return tmpl.render(dtmpl2)

    @cherrypy.expose
    def reuniaopg(self, **kwargs):
        tmpl = env.get_template('reuniaopg.html')
        dtmpl2 = dict()
        dtmpl2['session'] = cherrypy.session
        dtmpl2['sel_cli'] = self.getcombocli(0,'N')
        dtmpl2['form'] = dict()
        dtmpl2['erros'] = dict()
        dtmpl2['dados'] = sql_param.get_licenca_empr()
        return tmpl.render(dtmpl2)


    @cherrypy.expose
    def edservprecos(self, **kwargs):
        dtmpl2 = dict()
        dtmpl2['form'] = dict()
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(kwargs)
        fid = 0
        if kwargs.has_key('ID'):
            fid = int(kwargs['ID']) if kwargs['ID'] != '' else 0
            print 'Form'
            print fid
        if cherrypy.request.method == 'POST':
            print 'POST'
            if fid > 0:
                if kwargs['retstatus'] == 'apagar':
                    if sql_param.delete_servprecos(kwargs):
                        print 'Apagou'
                else:
                    if sql_param.update_servprecos(kwargs):
                        print 'Gravou'
            else:
                print 'Inserindo'
                if sql_param.insere_servprecos(kwargs):
                    print 'Inseriu'
            raise cherrypy.HTTPRedirect('/servprecos')
        if fid > 0:
            #
            #  PEGA A LISTAGEM DE SERVICOS
            #
            sql = ''' select SERVTABPRECO.*, depto.nm_setor
                ,case when servtabpreco.valor is not null then servtabpreco.valor
                 when servtabpreco.porcent is not null then servtabpreco.porcent  * geral.salario_minimo / 100
                 else 0
                 end as nvalor
                FROM SERVTABPRECO
                left join depto on depto.id_depto=servtabpreco.idsetor
                left join geral on geral.id = 1
                where SERVTABPRECO.ID = %(pid)s
                    order by SERVTABPRECO.DESCRICAO ''' % {'pid': fid}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql)
            lst_dig = contesputil.dictcursor3(cursorfb)
            pp.pprint(lst_dig)
            if lst_dig:
                dtmpl2['form'] = lst_dig[0]
        else:
            dtmpl2['form'] = {'ID': 0}

        tmpl = env.get_template('edservprecos.html')
        dtmpl2['session'] = cherrypy.session

        dtmpl2['erros'] = dict()
        dtmpl2['seldepto'] = sql_param.get_sel_depto()
        return tmpl.render(dtmpl2)

    @cherrypy.expose
    def ajuste_planfiscal(self, **kwargs):
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(kwargs)
        if kwargs.has_key('scodcli'):
            scodcli = kwargs['scodcli']
        else:
            scodcli = -1
        if kwargs.has_key('dt_fim'):
            strdataini = '01/' + kwargs['dt_fim']
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d/%m/%Y')
        datainicial = datetime.datetime.strptime(strdataini, '%d/%m/%Y')
        datafinal = contesputil.add_one_month(datainicial)
        selcompt = datainicial.strftime('%m/%Y')

        if cherrypy.request.method == 'POST':
            print 'post'
            fid = 0
            if kwargs.has_key('ID'):
                fid = int(kwargs['ID'])
                import modelbase
                if kwargs['retstatus'] == 'gravar':
                    if fid > 0:
                        print ('ID Maior '+str(fid))
                        fech_ajust = modelbase.SQLBASE(tabela='FECH_AJUSTE', generator='FECH_AJUSTE_ID_GEN',)
                        dados = kwargs
                        dados['COMPETENCIA'] = strdataini.replace('/', '.')
                        fech_ajust.upd_row(dados)
                    else:
                        print ('Id novo '+str(fid))
                        fech_ajust = modelbase.SQLBASE(tabela='FECH_AJUSTE', generator='FECH_AJUSTE_ID_GEN',)
                        dados = kwargs
                        del dados['ID']
                        dados['COMPETENCIA'] = strdataini.replace('/', '.')
                        fech_ajust.add_row(dados)
                if kwargs['retstatus'] == 'apagar':
                    if fid > 0:
                        modelbase.sql_delete('FECH_AJUSTE', 'ID', fid)

        dtmpl = dict()
        dtmpl['scompt'] = selcompt
        dtmpl['session'] = cherrypy.session
        dtmpl['scodcli'] = scodcli
        dtmpl['selcli'] = sql_param.get_sel_cliente()
        dtmpl['tbl_arq'] = sql_param.get_lst_ajusteplafech(strdataini, scodcli)
        if dtmpl['tbl_arq']:
            dtmpl['form'] = dtmpl['tbl_arq'][0]
        else:
            dtmpl['form'] = {}
        tmpl = env.get_template('ajuste_planfiscal.html')
        return tmpl.render(dtmpl)


    @cherrypy.expose
    def avisprot(self, **kwargs):
        idprot = 0
        codmd5 = ''
        if kwargs.has_key('q'):
            idprot = int(kwargs['q']) or 0
        if kwargs.has_key('s'):
            codmd5 = kwargs['s']
        cip = unicode(kwargs.get('ci') or '')
        if cip == u'':
            cip = cherrypy.request.remote.ip
        cip = cherrypy.request.remote.ip
        html = u'''
            <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                <title>Protocolo</title>
                <link rel="icon" href="./images/favicon.ico" type="image/x-icon" />
                <script type="text/javascript" src="./js/jquery/jquery.js"></script>
                <style rel="stylesheet" type="text/css">
                 body { font-family: Arial; font-size: 12px; }
                  th	{
                            background-color: black;
                            color: white;
                  }
                  pre {
                      background-color: #EEEEEE;
                      font-family: Arial;
                      font-size: 9px;
                      margin-bottom: 7px;
                      max-height: 600px;
                      overflow: auto;
                      padding: 5px;
                      max-width: 400px;
                  } 	
                  #dadosadd {
                    background-color: #DDDDDD;
                    font-family:Arial;
                    font-size: 9px;
                 }

                  .link_arq {
                    white-space: nowrap;
                    font-family:Arial;
                    font-size: 10px;
                 }

                  .textpeq {
                    font-family:Arial;
                    font-size: 11px;
                 }             

                  .tdlink_arq {
                    white-space: nowrap;
                 }             

                </style>
            </head>
            <body  >
            <div id="container" >

            <center>
            <div id="logo">
            <img src="./images/LOGOSITE4.jpg">	

            </div>
            '''
        html += u'<div id="welcome" ><small><center><br><br><br>IP = ' + unicode(
            cherrypy.request.remote.ip) + ' </center></small></div>'
        if (idprot == 0):
            html += '<h1><center><br><br><br>Lan&ccedil;amento de Protocolo n&atilde;o encontrado</center></h1>'
        else:
            ssql = '''
                    select protocolo2.*,depto.nm_setor as nm_setoruser from protocolo2
                    left join depto on depto.descricao=protocolo2.deptouser
                    where  protocolo2.idprot2= %(id)s and ((protocolo2.status = 'N') or (protocolo2.status ='B'))
                    ''' % {'id': idprot, 'md5': codmd5}
            cr_prot = contesputil.ret_cursor('fb')
            cr_prot.execute(ssql)
            dprot = contesputil.dictcursor2(cr_prot)
            if dprot:
                html += '''
                    <table border=1
                      style="width : 750 ; border-spacing: 2px;
                       border: 1px solid black; border-collapse: collapse;">
                    <tr><TD>Num.: %(IDPROT2)s</td><TD  colspan="4" >Cliente: %(CODCLIREP)s - %(NOME)s</td></tr>
                    <tr><td>Dt. Entregar: %(ENTREGA)s </td><td>Cadastrante: %(USUARIO)s </td><td>Setor: %(NM_SETORUSER)s </td><td>Status: %(STATUS)s</td>

                    </tr>
                    <tr><td>Para: %(RESPONSAVEL2)s </td><td colspan="4"> Assunto: %(ASSUNTO)s </td></tr>
                    </table>
                    ''' % dprot[0]

                if cherrypy.request.remote.ip[:7] <> '192.168':
                    ssql = '''
                            update protocolo2 set VISUALIZADO_ONLINE='S' , DATVISUALIZ=current_timestamp ,IPVISUALIZ = '%(ip)s'
                            where  protocolo2.idprot2= %(id)s and md5 = '%(md5)s' 
                            and (( VISUALIZADO_ONLINE='N' ) or ( VISUALIZADO_ONLINE is null ))
                            ''' % {'id': idprot, 'md5': codmd5, 'ip': cherrypy.request.remote.ip}
                # contesputil.execsql(ssql, 'fb')
                else:
                    ssql = '''
                            update protocolo2 set VISUALIZADO_ONLINE='N' , DATVISUALIZ=current_timestamp ,IPVISUALIZ = '%(ip)s'
                            where  protocolo2.idprot2= %(id)s and md5 = '%(md5)s' 
                            and (( VISUALIZADO_ONLINE='N' ) or ( VISUALIZADO_ONLINE is null ))
                            ''' % {'id': idprot, 'md5': codmd5, 'ip': cherrypy.request.remote.ip}
                # contesputil.execsql(ssql, 'fb')
                ssql = '''
                            select servicos2.* , tpserv.clirep as servcro
                                   ,tpserv.competencia as boolcomp, tpserv.venc as boolvenc,tpserv.valor as boolval
                                   ,tpserv.email as boolemail , tpserv.nome as servnome ,tpserv.descrprot as nomprot
                                   ,tpserv.prot as boolprot , tpserv.controlar, tpserv.msg , tpserv.TITULOAVULS from SERVICOS2
                            left outer join tpserv on (tpserv.idtpserv=servicos2.idservico)
                                                and (tpserv.prot='S')
                             where IDPROT2= %(id)s
                            order by SERVICOS2.IDPROTSERV ''' % {'id': idprot}
                cr_serv = contesputil.ret_cursor('fb')
                cr_serv.execute(ssql)
                dservicos = contesputil.dictcursor2(cr_serv)
                if dservicos:
                    html += '''
                        <table border=1
                          style="width : 750 ; border-spacing: 2px;
                           border: 1px solid black; border-collapse: collapse;">
                        <caption><strong>Documentos</strong></caption>
                        <tr>
                        <th>Documento</th><th>Compt.</th><th>Valor</th><th>Venc.</th><th class="tdlink_arq" >Arquivo</th>
                        </tr>
                        '''
                    for serv in dservicos:
                        html += '<tr>'
                        xnmserv = serv['SERVNOME']
                        if ((serv['DESCRAVULSA'] <> '') and (serv['DESCRAVULSA'])):
                            xnmserv += ''' <div id="dadosadd"><br> <strong>%(TITULOAVULS)s </strong> %(DESCRAVULSA)s </div> ''' % serv
                        if ((serv['OBS'] <> '') and (serv['OBS'])):
                            xnmserv += ''' <pre> %s </pre> ''' % serv['OBS']

                        html += '<td> %s </td>' % xnmserv

                        html += '<td class="textpeq"> %(c1)s/%(c2)s </td>' % {'c1': serv['COMPETENCIA'][0:2],
                                                                              'c2': serv['COMPETENCIA'][2:4]}
                        html += '<td class="textpeq" align="right"> %s </td>' % serv['VALOR']
                        html += '<td class="textpeq" > %s </td>' % serv['VENC']
                        if serv['IDDEPOSITO'] > 0:
                            html += '<td><a href="./docsdwnavis?i=%(idarq)s&m=%(pmd5)s&p=%(idp)s "> %(nome)s </a> </td>' % {
                                'idp': idprot, 'pmd5': codmd5, 'idarq': serv['IDDEPOSITO'], 'nome': serv['NMARQ']}
                        else:
                            ssql = ''' Select iddeposito,nmarquivo,lido_cliente,OCTET_LENGTH(deposito.conteudo)/1024 as tam from deposito where IDPROTOXOLO=%(idp)s and IDSERVPROT=%(idserv2)s ''' % {
                                'idp': idprot, 'idserv2': serv['IDPROTSERV']}
                            html += '<td class="tdlink_arq"><div class="link_arq"> <ul style="padding-left: 20px;"> \n'
                            cr_arq = contesputil.ret_cursor('fb')
                            cr_arq.execute(ssql)
                            darqserv = contesputil.dictcursor2(cr_arq)
                            for arq_deposito in darqserv:
                                dicarq = {'idp': idprot, 'pmd5': codmd5, 'idarq': arq_deposito['IDDEPOSITO'],
                                          'nome': arq_deposito['NMARQUIVO']}
                                if arq_deposito['LIDO_CLIENTE'] == 'S':
                                    dicarq['baixado'] = 'Baixado - %s KB' % arq_deposito['TAM']
                                    dicarq['liicon'] = 'disc'
                                else:
                                    dicarq['baixado'] = 'Nao Baixado - %s KB' % arq_deposito['TAM']
                                    dicarq['liicon'] = 'circle'
                                html += '<li style="list-style-type: %(liicon)s" ><a title="%(baixado)s" href="./docsdwnavis?i=%(idarq)s&m=%(pmd5)s&p=%(idp)s "> %(nome)s </a> </li>' % dicarq
                            html += '</ul> <div> </td> \n'

                        html += '</tr> \n'
                    html += '</table> \n'
            else:
                html += '<h1><center><br><br><br>Protocolo n&atilde;o localizado </center></h1>'
        html += '</center></div></body> </html>'
        return html

    @cherrypy.expose
    def docsdwnavis(self, **kwargs):
        cursorfb = contesputil.ret_cursor('fb')
        idprot = 0
        idarq = 0
        codmd5 = ''
        if kwargs.has_key('i'):
            idarq = int(kwargs['i']) or 0
        if kwargs.has_key('p'):
            idprot = int(kwargs['p']) or 0
        if kwargs.has_key('m'):
            codmd5 = kwargs['m']

        strcliatual = cherrypy.session.get('CODCLI_ATUAL') or '0'
        selcli = (int(strcliatual) or 0)
        sql = '''select deposito.iddeposito,deposito.data_entrada,deposito.usuario,deposito.nmarquivo,deposito.conteudo, OCTET_LENGTH(deposito.conteudo)
                 from deposito
                 inner join protocolo2 on protocolo2.idprot2=deposito.idprotoxolo
                 where deposito.iddeposito= %(iarq)d and deposito.IDPROTOXOLO = %(iprot)d  ''' % {
            'iarq': int(idarq), 'iprot': idprot, 'md5': codmd5}
        cursorfb.execute(sql.encode('ascii'))
        rs_arq = cursorfb.fetchall()
        arq_down = None
        nmarq = ''
        size = 0
        if rs_arq:
            nmarq = rs_arq[0][3]
            arq_down = rs_arq[0][4]
            size = rs_arq[0][5]
        if arq_down:
            cherrypy.response.headers["Content-Type"] = "application/x-download"
            cherrypy.response.headers["Content-Disposition"] = 'attachment; filename="%s"' % nmarq
            cherrypy.response.headers["Content-Length"] = size
            if cherrypy.request.remote.ip[:7] <> '192.168':
                sql = '''update deposito set lido_cliente='S' , download='%(ipcli)s' , datdown=current_timestamp
                         where deposito.iddeposito=%(iarq)d and datdown is null  ''' % {'iarq': int(idarq),
                                                                                        'ipcli': cherrypy.request.remote.ip}
            # contesputil.execsql(sql, 'fb')

            sql = '''update deposito set ultdown='%(ipcli)s' , datultdown=current_timestamp
                     where deposito.iddeposito=%(iarq)d   ''' % {'iarq': int(idarq),
                                                                 'ipcli': cherrypy.request.remote.ip}
            # contesputil.execsql(sql, 'fb')
            BUF_SIZE = 1024 * 5
            return cherrypy.lib.static.serve_fileobj(arq_down, content_type="application/x-download",
                                                     disposition="attachment", name=nmarq)
        else:
            return "Arquivo nao encontrado"

    @cherrypy.expose
    def verif_fech_fiscal(self, **kwargs):
        selcli = 0
        if kwargs.has_key('idcli'):
            selcli = (int(kwargs['idcli']) or 0)
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        selcompt = strdataini[3:].replace('.', '/')
        datainicial = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        if kwargs.has_key('dt_fim'):
            strdatafim = '01.' + kwargs['dt_fim'].replace('/', '.')
        else:
            strdatafim = contesputil.add_one_month(datainicial).strftime('%d.%m.%Y')
        selcomptfim = strdatafim[3:].replace('.', '/')
        datafinal = datetime.datetime.strptime(strdatafim, '%d.%m.%Y')
        dtmpl = dict()
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''select cli_coddominio,razao from cliente where status='A' order by razao '''
        cursorfb.execute(sql.encode('ascii'))
        rs_cli = cursorfb.fetchall()
        htmlcli = u''
        for cli_atual in rs_cli:
            if cli_atual[0] == selcli:
                selec = u'selected'
            else:
                selec = u''
            htmlcli += u'<option value="%(cod)s" %(sel)s >%(nome)s</option>' % {'cod': cli_atual[0], 'sel': selec,
                                                                                'nome': cli_atual[1].decode('latin-1',
                                                                                                            'replace')}
        dtmpl['sel_cli'] = htmlcli
        dtmpl['scompt'] = selcompt
        dtmpl['scomptfim'] = selcomptfim
        lst_verif = list()
        if selcli > 0:
            compttmp = datainicial
            while compttmp <= datafinal:
                lst_verif.append(sql_param.get_verif_apuracao_fech_fiscal(selcli, compttmp))
                compttmp = contesputil.add_one_month(compttmp)
        dtmpl['tbl_arq'] = lst_verif
        dtmpl['session'] = cherrypy.session
        tmpl = env.get_template('verif_fech_fiscal.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def servprecos(self, **kwargs):
        #
        #  PEGA A LISTAGEM DE SERVICOS
        #
        sql = ''' select SERVTABPRECO.*, coalesce(depto.nm_setor,'GERAL') AS NM_SETOR
            ,case when servtabpreco.valor is not null then servtabpreco.valor
             when servtabpreco.porcent is not null then servtabpreco.porcent  * geral.salario_minimo / 100
             else 0
             end as nvalor
            FROM SERVTABPRECO
            left join depto on depto.id_depto=servtabpreco.idsetor
            left join geral on geral.id = 1
                order by depto.nm_setor,SERVTABPRECO.DESCRICAO'''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_dig = contesputil.dictcursor3(cursorfb)
        #
        #  PEGA VALOR DO SALARIO MINIMO
        #
        sql = ''' select  geral.salario_minimo FROM geral
            WHERE geral.id = 1	'''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_sal = contesputil.dictcursor3(cursorfb)
        if lst_sal:
            vsal = lst_sal[0]['SALARIO_MINIMO']
        else:
            vsal = 0.0

        dtmpl = dict()
        dtmpl['session'] = cherrypy.session
        dtmpl['tbl_arq'] = lst_dig
        dtmpl['salario'] = vsal
        dtmpl['seldepto'] = sql_param.get_sel_depto()
        tmpl = env.get_template('servprecos.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_qtdlanccontabil_tmp(self, **kwargs):
        lst_lanc = list()
        lst_lcli = list()
        if kwargs.has_key('dt_ini'):
            strdataini = '01.' + kwargs['dt_ini'].replace('/', '.')
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d.%m.%Y')
        d = datetime.datetime.strptime(strdataini, '%d.%m.%Y')
        datafinal = contesputil.add_one_month(d)
        strdatafim = datafinal.strftime("%d/%m/%Y").replace('/', '.')
        sdodataini = strdataini[6:10] + strdataini[3:5] + strdataini[0:2]
        sdodatafim = strdatafim[6:10] + strdatafim[3:5] + strdatafim[0:2]
        cursorfb = contesputil.ret_cursor('do')
        sql = '''SELECT upper(codi_usu), count(1),codi_emp
         FROM bethadba.ctlancto WITH (NOLOCK)
         where dorig_lan >= '%(dtini)s' and dorig_lan <= '%(dtfim)s'
          group by codi_usu , codi_emp ORDER by codi_usu ''' % {'dtini': sdodataini, 'dtfim': sdodatafim}
        cursorfb.execute(sql.encode('ascii'))
        rowsetfb = cursorfb.fetchall()
        for row in rowsetfb:
            usuarios = [item for item in lst_lanc if (item.nome == row[0]) and (item.cliente == row[2])]
            if not usuarios:
                usuario = qtdlanc(row[0])
                usuario.entrada = row[1]
                usuario.cliente = row[2]
                lst_lanc.append(usuario)
            else:
                usuario = usuarios[0]
                usuario.entrada = usuario.entrada + row[1]

        d = list()
        for item in lst_lanc:
            d.append(str(item.nome) +
                     '</td><td>' + str(item.total()))
        return d

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_senhaleg(self, **kwargs):
        if kwargs.has_key('IDS'):
            ids = int(kwargs['IDS'])
        else:
            ids = 0
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''SELECT * from LEG_SENHAS where ID = %(ID)s''' % {'ID': ids}
        cursorfb.execute(sql.encode('ascii'))
        d = contesputil.dictcursor2(cursorfb)
        return d

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_validaremail(self, **kwargs):
        if kwargs.has_key('ID'):
            ids = int(kwargs['ID'])
        else:
            ids = 0
        if kwargs.has_key('VALID'):
            validar = kwargs['VALID']
        else:
            validar = ''
        ret = 0
        print ('tbl_validaremail')
        print (kwargs)

        if ids > 0:
            dusersql = 'Select * from WEBUSERVERIF where id = %(ID)s ' % {'ID': ids}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(dusersql.encode('ascii'))
            d = contesputil.dictcursor2(cursorfb)
            if d:
                duser = d[0]
                if validar == 'S':
                    sql = '''insert into WEBUSERVALID (EMAIL,IP,GNID)
                      values ('%(EMAIL)s','%(IP)s','%(GNID)s') 
                    ''' % {'EMAIL': duser['EMAIL'], 'IP': duser['IP'], 'GNID': duser['GNID']}
                    ret = contesputil.execsql(sql.encode('ascii'), 'fb')
                if validar == 'N':
                    sql = '''delete from WEBUSERVALID 
                      where EMAIL='%(EMAIL)s' and IP='%(IP)s' and GNID='%(GNID)s' 
                    ''' % {'EMAIL': duser['EMAIL'], 'IP': duser['IP'], 'GNID': duser['GNID']}
                    print (sql)
                    ret = contesputil.execsql(sql.encode('ascii'), 'fb')
        return ret



    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_licencaleg(self, **kwargs):
        if kwargs.has_key('IDS'):
            ids = int(kwargs['IDS'])
        else:
            ids = 0
        cursorfb = contesputil.ret_cursor('fb')
        sql = '''   select LICENCAEMPRESA.*, cliente.razao from LICENCAEMPRESA
        left join cliente on cliente.codigo=LICENCAEMPRESA.CODCLI 
        where id = %(ID)s
        ''' % {'ID': ids}
        cursorfb.execute(sql.encode('ascii'))
        d = contesputil.dictcursor2jsn(cursorfb)
        return d


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tbl_licencalegdelete(self, **kwargs):
        if kwargs.has_key('IDS'):
            ids = int(kwargs['IDS'])
        else:
            ids = 0
        ret = 0
        if ids > 0:
            sql = '''delete from LICENCAEMPRESA 
            where id = %(ID)s
            ''' % {'ID': ids}
            ret = contesputil.execsql(sql.encode('ascii'), 'fb')
        return ret


    @cherrypy.expose
    def tributacaoviged(self, **kwargs):
        dtmpl2 = dict()
        dtmpl2['form'] = {'CODCLI': 0 }
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(kwargs)
        fid = 0
        if kwargs.has_key('PID'):
            fid = int(kwargs['PID']) if kwargs['PID'] != '' else 0
            print 'Form'
            print fid
        if cherrypy.request.method == 'POST':
            print 'POST'
            kwargs['ID_USER'] = cherrypy.session.get('IDUSER',0)
            if fid > 0:
                if kwargs['retstatus'] == 'apagar':
                    if sql_param.delete_trib(kwargs):
                        print 'Apagou'
                else:
                    if sql_param.update_trib(kwargs):
                        print 'Gravou'
            else:
                print 'Inserindo'
                if sql_param.insere_trib(kwargs):
                    print 'Inseriu'
            raise cherrypy.HTTPRedirect('/tributacaovig')
        if fid > 0:
            #
            #  PEGA A LISTAGEM DE SERVICOS
            #
            sql = ''' select TRIBUTACAO_VIG.*,cliente.razao, TPTRIBUTACAO.nome as NMTPTRIB from TRIBUTACAO_VIG
            left join cliente on cliente.codigo=tributacao_vig.codcli
            left join tptributacao on tptributacao.id=tributacao_vig.ID_TPTRIBUTACAO
            where tributacao_vig.id =  %(pid)s
            order by cliente.razao,tributacao_vig.dat_vig desc      ''' % {'pid': fid}
            cursorfb = contesputil.ret_cursor('fb')
            cursorfb.execute(sql)
            lst_dig = contesputil.dictcursor3(cursorfb)
            pp.pprint(lst_dig)
            if lst_dig:
                dtmpl2['form'] = lst_dig[0]
        else:
            ini_mes = datetime.date.today().replace(day=1)
            strdataini = ini_mes.strftime('%d/%m/%Y')
            dtmpl2['form'] = {'ID': 0, 'CODCLI': 0,'DAT_VIG':strdataini}

        #
        #  Lista empresas
        #
        sql = ''' select cliente.codigo, cliente.razao, cliente.status from cliente
            where cliente.status='A'
            order by cliente.razao'''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_cli = list()
        for row in cursorfb.fetchall():
            lst_cli.append((row[0], row[1].decode('latin-1')))

        #
        #  Lista Tributação
        #
        sql = ''' select ID,NOME from TPTRIBUTACAO order by NOME'''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_trib = list()
        for row in cursorfb.fetchall():
            lst_trib.append((row[0], row[1].decode('latin-1')))

        tmpl = env.get_template('tributacaoviged.html')
        dtmpl2['session'] = cherrypy.session
        dtmpl2['erros'] = dict()
        dtmpl2['lst_cli'] = lst_cli
        dtmpl2['lst_trib'] = lst_trib
        dtmpl2['SELCLI'] = dtmpl2['form']['CODCLI']
        print 'Selcli'
        print dtmpl2['SELCLI']
        return tmpl.render(dtmpl2)



    @cherrypy.expose
    def tributacaovig(self, **kwargs):
        #
        #  Cadastro de tribução por vigencia
        #
        print kwargs
        if kwargs.has_key('SELCLI'):
            selcli = int(kwargs['SELCLI'])
        else:
            selcli = 0

        if kwargs.has_key('dt_ano'):
            selano = int(kwargs['dt_ano'])
        else:
            hj = datetime.date.today()
            selano = int(hj.year)

        if kwargs.has_key('acao'):
            selacao = kwargs['acao']
            if selacao == 'Novo':
                #
                #  Lista empresas
                #
                sql = ''' select cliente.codigo, fechtrib.tributacao, anttrib.id_tptributacao from cliente
                    left join (
                    select distinct fech_imp_recolher.codcli,fech_imp_recolher.tributacao from fech_imp_recolher
                    where extract(year from fech_imp_recolher.competencia)=  %(pselano)s
                    ) as fechtrib on fechtrib.codcli = cliente.codigo
                    
                    left join (
                    select TRIBUTACAO_VIG.codcli, tributacao_vig.id_tptributacao from TRIBUTACAO_VIG
                                        where extract (year from tributacao_vig.dat_vig)+1 =  %(pselano)s
                    ) as anttrib on anttrib.codcli = cliente.codigo
                                        where cliente.status='A'
                                        order by cliente.codigo
                        '''  % {'pselano': selano}
                cursorfb = contesputil.ret_cursor('fb')
                cursorfb.execute(sql)
                clientes = contesputil.dictcursor3(cursorfb)

                sql = ''' select TRIBUTACAO_VIG.codcli from TRIBUTACAO_VIG
                    where extract (year from tributacao_vig.dat_vig) = %(pselano)s order by codcli ''' % {'pselano': selano}
                cursorfb = contesputil.ret_cursor('fb')
                cursorfb.execute(sql)
                lst_trib = contesputil.dictcursor3(cursorfb)

                for cli in clientes:
                    tribencontrada = next((x for x in lst_trib if x['CODCLI'] == cli['CODIGO']), None)
                    if tribencontrada is None:
                        dtrib = {'SELCLI': cli['CODIGO'], 'ID_TPTRIBUTACAO': None, 'DAT_VIG': '01/01/'+str(selano), 'ID_USER': cherrypy.session.get('IDUSER',0)}
                        if cli['ID_TPTRIBUTACAO']:
                            dtrib['ID_TPTRIBUTACAO'] = cli['ID_TPTRIBUTACAO']
                        else:
                            if cli['TRIBUTACAO'] == 'Simples Nacional':
                                dtrib['ID_TPTRIBUTACAO'] = 1
                            if cli['TRIBUTACAO'] == 'Lucro Real':
                                dtrib['ID_TPTRIBUTACAO'] = 4
                            if cli['TRIBUTACAO'] == 'Lucro Presumido':
                                dtrib['ID_TPTRIBUTACAO'] = 3

                        sql_param.insere_trib(dtrib)
            if selacao == 'Apagar':
                inssql = '''
                delete from TRIBUTACAO_VIG 
                where extract (year from tributacao_vig.dat_vig) = %(pselano)s ''' % {'pselano': selano}
                print inssql
                param = {}
                contesputil.execsqlp(inssql, param, 'fb')

        sql = ''' select TRIBUTACAO_VIG.*,cliente.razao, TPTRIBUTACAO.nome as NMTPTRIB from TRIBUTACAO_VIG
            left join cliente on cliente.codigo=tributacao_vig.codcli
            left join tptributacao on tptributacao.id=tributacao_vig.ID_TPTRIBUTACAO
            where extract (year from tributacao_vig.dat_vig) = %(pselano)s 
            order by cliente.razao,tributacao_vig.dat_vig desc ''' % {'pselano': selano}
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_dig = contesputil.dictcursor3(cursorfb)
        #
        #  Lista empresas
        #
        sql = ''' select cliente.codigo, cliente.razao, cliente.status from cliente
            where cliente.status='A'
            order by cliente.razao'''
        cursorfb = contesputil.ret_cursor('fb')
        cursorfb.execute(sql)
        lst_cli = list()
        for row in cursorfb.fetchall():
            lst_cli.append((row[0], row[1].decode('latin-1')))
        #Exemplo utilado para filtrar por cliente
        #{{ select_ini('SELCLI',options=lst_cli,default=SELCLI,size=5,titulo='') }}
        dtmpl = dict()
        dtmpl['session'] = cherrypy.session
        dtmpl['tbl_arq'] = lst_dig
        dtmpl['lst_cli'] = lst_cli
        dtmpl['selcli'] = selcli
        dtmpl['scomptfim'] = selano
        #dtmpl['seldepto'] = sql_param.get_sel_depto()
        tmpl = env.get_template('tributacaovig.html')
        return tmpl.render(dtmpl)

    @cherrypy.expose
    def uploadconvertpdftxt(self, **kwargs):
        # Either save the file to the directory where server.py is
        # or save the file to a given path:
        # upload_path = '/path/to/project/data/'
        print 'upload'
        print kwargs
        if kwargs:
            print 'tem'
            upload_path = r'/mnt/divers/pdfconv/inarq'
            download_path = r'/mnt/divers/pdfconv/outarq'
            ufile = kwargs['ufile']
            # Save the file to a predefined filename
            # or use the filename sent by the client:
            upload_filename = ufile.filename
            #upload_filename = 'saved.txt'

            upload_file = os.path.normpath(
                os.path.join(upload_path, upload_filename))
            size = 0
            with open(upload_file, 'wb') as out:
                while True:
                    data = ufile.file.read(8192)
                    if not data:
                        break
                    out.write(data)
                    size += len(data)
            out = '''
            <br>Arquivo Recebido.
            <br>Arquivo: {}
            <br>Tamanho: {}
            <br>Mime-type: {}
            '''.format(ufile.filename, size, ufile.content_type, data)

            if upload_filename[-4:].lower() == '.pdf':
                outfilename = upload_filename[:-4] + '.txt'
                out_file = os.path.normpath(
                    os.path.join(download_path, outfilename))
                import contesp_convpdf
                if os.path.exists(out_file):
                    os.remove(out_file)
                if contesp_convpdf.convertfile(upload_file,out_file):
                    out += '<br>Arquivo gerado: ' + outfilename
                    if os.path.exists(upload_file):
                        os.remove(upload_file)
                    return cherrypy.lib.static.serve_file(out_file, 'application/x-download',
                                             'attachment', os.path.basename(out_file))
        else:
            print 'nao tem'
            tmpl = env.get_template('uploaddpf.html')
            out = tmpl.render()
        return out

    @cherrypy.expose
    def rssverprot(self, **kwargs):
        out = '''<?xml version="1.0" encoding="UTF-8" ?>
        <rss version="2.0">
            <channel>
                <title>Protocolos em aberto</title>
                <link>http://192.168.0.124:5000/verifprot</link>
                <description>Mostra os protocolos verificados hoje</description>
                <language>pt-br</language>
                <copyright>Site - Todos os direitos reservados.</copyright>
                <image>
                    <title>Logo Contesp</title>
                    <url>http://icontesp/images/logo_CONTROLE_fech.JPG</url>
                    <link>http://192.168.0.124:5000/verifprot</link>
                </image>
                
                <ttl>20</ttl>
        '''
        ini_mes = datetime.date.today()
        strdataini = ini_mes.strftime('%d.%m.%Y')
        datafinal = contesputil.add_one_month(ini_mes)
        strdatafim = datafinal.strftime("%d.%m.%Y")

        vprot = sql_param.get_verifprot(strdataini, strdatafim)
        for prot in vprot:
            out += '''  
                        <item>
                            <title>%(EMAIL)s - %(DTCAD)s</title>
                            <link>http://192.168.0.124:5000/verifprot</link>
                            <description>%(IDPROT)s - %(EMAIL)s - %(GNID)s</description>
                            <datePosted>%(DTCAD)s</datePosted>
                        </item>
            ''' % {'DTCAD': prot['DTCAD'], 'EMAIL': prot['EMAIL'], 'IDPROT': prot['IDPROT'], 'GNID': prot['GNID']}
        out += '''
            </channel>
        </rss>
        '''
        cherrypy.response.headers['Content-Type'] = 'application/rss+xml'
        return out



    @cherrypy.expose
    def uploadprotocolo(self, **kwargs):
        # Either save the file to the directory where server.py is
        # or save the file to a given path:
        # upload_path = '/path/to/project/data/'
        print 'upload'
        print kwargs
        if kwargs:
            print 'tem'
            ufile = kwargs['file']
            # Save the file to a predefined filename
            # or use the filename sent by the client:
            upload_filename = ufile.filename
            print upload_filename
            size = 0
            pdados = ('LEANDRO_TI', ufile.file, upload_filename, None, 0, 'S')
            insql = '''
                insert into DEPOSITO (USUARIO,CONTEUDO,NMARQUIVO,DESVRICAO,CODCLI,AVISA_CLI)
                              values (?      ,?       ,?        ,?        ,?     ,? )     
            '''
            contesputil.execsqlp(insql, pdados, 'fb')
            out = {"status": "success", "path": upload_filename, "name": upload_filename}
            rt = json.dumps(out)
            cherrypy.response.headers['Content-Type'] = 'application/javascript'
            cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
            print rt
            return rt
        else:
            print 'nao tem'
            tmpl = env.get_template('uploadprot.html')
            out = tmpl.render()
        return out



def set_procname(newname):
    from ctypes import cdll, byref, create_string_buffer
    libc = cdll.LoadLibrary('libc.so.6')  # Loading a 3rd party library C
    buff = create_string_buffer(len(newname) + 1)  # Note: One larger than the name (man prctl says that)
    buff.value = newname  # Null terminated string as it should be
    libc.prctl(15, byref(buff), 0, 0,
               0)  # Refer to "#define" of "/usr/include/linux/prctl.h" for the misterious value 16 & arg[3..5] are zero as the man page says.


dthandler = lambda obj: obj.strptime(v, '%Y-%m-%d') if isinstance(obj, datetime.datetime) else json.JSONEncoder.default(
    self, obj)

if __name__ == '__main__':
    import os.path

    thisdir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, thisdir)
    # Add app dir to path for testing cpcgiserver
    APPDDIR = os.path.abspath(os.path.join(thisdir, os.path.pardir, os.path.pardir))
    sys.path.insert(0, APPDDIR)

    # locale.setlocale(locale.LC_ALL,'pt_BR.utf8')
    # locale.setlocale(locale.LC_ALL,'')
    cherrypy.tree.mount(rel_desemp(), config=thisdir + '/setup.conf')
    cherrypy.config.update(thisdir + '/setup.conf')

    cherrypy.engine.start()
    cherrypy.engine.block()
