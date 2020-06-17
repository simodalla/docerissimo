import os.path
import cx_Oracle
import mysql.connector

STORE_ROOT_PATH="/opt/alfresco-4.0.d/alf_data/contentstore/"

tpl_sql_props = "SELECT qname.local_name, props.long_value, props.string_value \
FROM alfresco.alf_node_properties as props \
INNER JOIN alfresco.alf_qname as qname on qname.id = props.qname_id \
WHERE props.node_id= {} \
AND qname.local_name in ('docnum', 'content', 'tipoComponente', \
 'numPg', 'docnumPrinc', 'codEnte', 'codAoo', 'dataCreazione')"

tpl_sql_store_url = "SELECT props.node_id, qn.local_name, acd.content_url_id, acu.content_url \
FROM alf_node_properties as props \
inner join alf_qname as qn on props.qname_id = qn.id \
inner join alf_content_data acd on acd.id = props.long_value \
inner join alf_content_url acu on acu.id = acd.content_url_id \
where node_id in ( {} ) \
and qn.local_name = 'content'"

tpl_sql_allegati = "SELECT qname.local_name, props.* \
FROM alfresco.alf_node_properties as props \
INNER JOIN alfresco.alf_qname as qname on qname.id = props.qname_id \
WHERE props.string_value= '{}'"

tpl_akrop_dati = """SELECT a.*,
    (select LISTAGG(A.dl_ogg || ' (' || C.DB_ID_FILE_REP_RMT || ')', ' - ') WITHIN GROUP (ORDER BY C.DB_ID_FILE_REP_RMT) "CORRELATI"
        from PAG_ALG_ALG A, PPR_PRT_ALG_INFORM B, PAG_ALG_REP C
        where A.ID_ALG = B.ID_FK_ALG and C.ID = A.ID_ALG_REP and (C.FL_ALLIN = 'S') and (C.CD_REP = 'DOCER')
            and (A.TI_MEM = 'M') and (B.FL_DOC_PRINC = 'N') and (B.ID_PROT = a.id_prot))   
    FROM ppr_prt_v_alg_reg_docer a WHERE a.pg_prot={} and a.aa_prot = {}"""


class DocerAnalyer:

    def __init__(self, docer_doc_num=None, prot_num=None, year=2020, debug=False):
        self.docer_doc_num = docer_doc_num
        self.prot_num = prot_num
        self.year = year
        self.debug = debug
        self.docer_data = {self.docer_doc_num: {}} if self.docer_doc_num else {}
        self.alf_cursor = None

    def bootstrap(self, alf_host, alf_user, alf_password, alf_database,
                  akro_dsn, akro_user, akro_password):
        self.alf_connection = mysql.connector.connect(
            host=alf_host,  user=alf_user, password=alf_password, database=alf_database)
        self.alf_cursor = self.alf_connection.cursor()
        self.akro_connection = cx_Oracle.connect(akro_user, akro_password, akro_dsn)
        self.akro_cursor = self.akro_connection.cursor()

    def get_properties_by_docer_number(self, docer_doc_num):
        self.alf_cursor.execute(tpl_sql_props.format(docer_doc_num))
        return self.alf_cursor.fetchall()

    def get_store_info(self, docer_doc_num):
        self.alf_cursor.execute(tpl_sql_store_url.format(docer_doc_num))
        return self.alf_cursor.fetchall()

    def get_allegati(self, docer_doc_num):
        self.alf_cursor.execute(tpl_sql_allegati.format(docer_doc_num))
        return self.alf_cursor.fetchall()

    def get_akro_allegati(self, prot_num, year):
        self.akro_cursor.execute(tpl_akrop_dati.format(prot_num, year))
        return self.akro_cursor.fetchall()

    def set_properties_by_docer_number(self):
        res_props = self.get_properties_by_docer_number(self.docer_doc_num)
        for prop in res_props:
            self.docer_data[self.docer_doc_num].update(
                {prop[0]: {'long_value': prop[1], 'string_value': prop[2]}})

    def set_properties_of_principal(self):
        if self.is_docer_doc_principal():
            return False
        res = self.get_properties_by_docer_number(
            self.docer_data[self.docer_doc_num]['docnumPrinc']['string_value'])
        self.docer_data[self.docer_doc_num].update({'principale': {}})
        for prop in res:
            self.docer_data[self.docer_doc_num]['principale'].update(
                {prop[0]: {'long_value': prop[1], 'string_value': prop[2]}})

    def is_docer_doc_principal(self):
        if (self.docer_doc_num and self.docer_doc_num in self.docer_data
             and 'tipoComponente' in self.docer_data[self.docer_doc_num] and
                'string_value' in self.docer_data[self.docer_doc_num]['tipoComponente'] and
                self.docer_data[self.docer_doc_num]['tipoComponente']['string_value'] == 'PRINCIPALE'):
            return True
        return False

    def set_store_url_principal(self):
        res = self.get_store_info(self.docer_doc_num)
        # print(res)
        for data in res:
            self.docer_data[self.docer_doc_num].update(
                {'path': os.path.join(STORE_ROOT_PATH, data[3].split('//')[1])})

    def set_allegati(self):
        res_allegati = self.get_allegati(self.docer_doc_num)
        self.docer_data[self.docer_doc_num].update({'allegati': {}})
        for data in res_allegati:
            allegato_docer_num = data[1]
            self.docer_data[self.docer_doc_num]['allegati'].update({allegato_docer_num: {}})
            res_allegato_store = self.get_store_info(allegato_docer_num)
            self.docer_data[self.docer_doc_num]['allegati'][allegato_docer_num].update(
                {'path': os.path.join(STORE_ROOT_PATH, res_allegato_store[0][3].split('//')[1])})

    def _parse_akro_data_allegati(self, data):
        if data:
            return [(x[::-1].split('(')[0].strip()[::-1].strip(")"), x[::-1].split('(')[1][::-1]) for x in data.split(") -")]
        return []

    def set_akro_data(self):
        akro_dati = self.get_akro_allegati(
            self.docer_data[self.docer_doc_num]['numPg']['string_value'],
            self.docer_data[self.docer_doc_num]['dataCreazione']['string_value'][:4])
        if not len(akro_dati) == 1:
            raise Exception("errore on set_akro_data")
        akro_dati = akro_dati[0]
        self.docer_data[self.docer_doc_num].update({'akro_nome_file': akro_dati[1]})
        if 'allegati' not in self.docer_data[self.docer_doc_num]:
            self.docer_data[self.docer_doc_num].update({'allegati': {}})
        if self.debug:
            print("*** --> {}".format(akro_dati))
        self.parsed_akro_data = self._parse_akro_data_allegati(akro_dati[-1])
        if self.debug:
            print("-------------")
            print(self.parsed_akro_data)
            print(self.docer_data[self.docer_doc_num]['allegati'])
            print("-------------")
        for allegato_docer_num, nome_file in self.parsed_akro_data:
            try:
                self.docer_data[self.docer_doc_num]['allegati'][int(allegato_docer_num)].update(
                    {'akro_nome_file': nome_file.strip()})
            except KeyError:
                self.docer_data[self.docer_doc_num]['allegati'].update(
                    {int(allegato_docer_num): {
                        'akro_nome_file': nome_file.strip(),
                        'path':"_________NUMERO DOCER RISULTA SU AKROPOLIS MA NON SU DOCER_________"}})

    def format_console_output(self):
        out = "RISULTATI ANALISI\n"
        out += "docer_doc_num:{}\n".format(self.docer_doc_num)
        out += "tipo_documento_docer:{}\n".format(
            self.docer_data[self.docer_doc_num]['tipoComponente']['string_value'])
        out += "cod_ente:{}\n".format(
            self.docer_data[self.docer_doc_num]['codEnte']['string_value'])
        out += "cod_aoo:{}\n".format(
            self.docer_data[self.docer_doc_num]['codAoo']['string_value'])
        out += "data_creazione:{}\n".format(
            self.docer_data[self.docer_doc_num]['dataCreazione']['string_value'])
        if self.is_docer_doc_principal():
            out += "prot_num:{}\n".format(
                self.docer_data[self.docer_doc_num]['numPg']['string_value'])
            out += "doc_num_prin:{}\n".format(self.docer_doc_num)
            try:
                out += "akro_nome_file:{}\n".format(self.docer_data[self.docer_doc_num]['akro_nome_file'])
            except KeyError:
                out += "akro_nome_file:___________ERRORE-NO-CORRISPONDENZA-CON-AKROPOLIS________________\n"
        else:
            # TODO:
            out += "prot_num:{}\n".format(self.docer_data[self.docer_doc_num]['principale']['numPg']['string_value'])
            out += "doc_num_prin:{}\n".format(self.docer_data[self.docer_doc_num]['docnumPrinc']['string_value'])
        out += "fs_path:{}\n".format(
            self.docer_data[self.docer_doc_num]['path'])
        if 'allegati' in self.docer_data[self.docer_doc_num]:
            allegati_nums = self.docer_data[self.docer_doc_num]['allegati'].keys()
            out += "num_allegati:{}\n".format(len(allegati_nums))
            if not self.parsed_akro_data:
                out += "info_query_allegati_akropolis:___________ERRORE-LA-DEGLI_ALLEGATI-SU-AKROPOLIS-NON-DA-RISULTATI_______________\n"
            else:
                out += "info_query_allegati_akropolis:OK\n"
            for n, allegato in enumerate(allegati_nums):
                out += "allegato_{}_doc_num:{}\n".format(n + 1, allegato)
                out += "allegato_{}_fs_path:{}\n".format(
                    n + 1, self.docer_data[self.docer_doc_num]['allegati'][allegato]['path'])
                try:
                    out += "allegato_{}_akro_nome_file:{}\n".format(
                        n + 1, self.docer_data[self.docer_doc_num]['allegati'][allegato]['akro_nome_file'])
                except KeyError:
                    out += "allegato_{}_akro_nome_file:___________ERRORE-NO-CORRISPONDENZA-CON-AKROPOLIS________________\n".format(n+1)
        else:
            out += "num_allegati:0\n"

        return out


# tp_sql_properties_protocollo = "SELECT node.*, qname.local_name, props.long_value, props.string_value \
#tp_sql_properties_protocollo = "SELECT node.*, qname.local_name, props.string_value \
tp_sql_properties_protocollo = "SELECT node.id, props.string_value \
FROM alfresco.alf_node as node \
INNER JOIN alfresco.alf_node_properties as props on node.id = props.node_id \
INNER JOIN alfresco.alf_qname as qname on qname.id = props.qname_id \
WHERE qname.local_name = 'registroPg' and props.string_value='{}'"

#"SELECT * FROM alfresco.alf_node_properties where string_value='PROTOCOLLO'";

class DocerBonify:
    def __init__(self):
        self.rs_node_id_by_registro = []

    def bootstrap(self, alf_host, alf_user, alf_password, alf_database):
        self.alf_connection = mysql.connector.connect(
            host=alf_host,  user=alf_user, password=alf_password, database=alf_database)
        self.alf_cursor = self.alf_connection.cursor()


    def set_nodes_by_registro(self, registro='PROT', limit=0, doc_numbers=None):
        sql = tp_sql_properties_protocollo.format(registro)
        if doc_numbers:
            sql = "{} and node.id in ({})".format(sql, doc_numbers)
        if int(limit) > 0:
            sql = "{} LIMIT {}".format(sql, limit)
        #print("**** {}".format(sql))
        self.alf_cursor.execute(sql)
        rs = self.alf_cursor.fetchall()
        print(rs)
        print("***")
        self.rs_node_id_by_registro = [str(data[0]) for data in rs if data[1] == registro]

    def bonify_registro(self, registro_from='PROTOCOLLO', registro_to='PROT'):

        sql = "UPDATE alfresco.alf_node_properties SET string_value='{}' " \
              "WHERE node_id in ({}) " \
              "and qname_id = 304 " \
              "and string_value='{}'" .format(registro_to, ','.join(self.rs_node_id_by_registro), registro_from)
        print("bbbb-> " + sql)
        # rs = self.alf_cursor.execute(sql)
        # print(rs)
                



"""
[(871280, 12, 6, u'970b61c0-8cf5-4b0f-82f3-bceee25d031c', 18520944, 0, 249, 2, 1705318, u'ml.capelli', u'2017-07-17T17:25:03.029+02:00', u'admin', u'2019-12-24T18:44:22.287+01:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(872257, 12, 6, u'3ca760e1-506f-4e47-aaad-514e2cc50a81', 18520945, 0, 249, 2, 1707264, u'ml.capelli', u'2017-07-18T11:30:46.232+02:00', u'admin', u'2019-12-24T18:44:33.090+01:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(925227, 12, 6, u'a97dea99-8c10-45ab-a9a7-4dc2e05a1195', 18520943, 0, 249, 2, 1810416, u'ml.capelli', u'2017-08-29T12:39:45.975+02:00', u'admin', u'2019-12-24T18:44:11.527+01:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(939092, 9, 6, u'e37b4b67-caab-4815-a306-3c6b0d77894f', 6684528, 0, 249, 2, 1836216, u'admin_docer', u'2017-09-08T12:32:08.210+02:00', u'admin_docer', u'2017-09-08T12:34:16.751+02:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(958658, 9, 6, u'5b153503-aa9d-42af-96d2-2cb58d7a3c3c', 6827884, 0, 249, 2, 1875244, u'admin_docer', u'2017-09-21T15:14:38.145+02:00', u'admin_docer', u'2017-09-21T15:18:34.704+02:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(978578, 9, 6, u'81be99d8-737a-4bed-bf9d-855fd99523c3', 6955449, 0, 249, 2, 1915002, u'admin_docer', u'2017-09-29T11:50:45.037+02:00', u'admin_docer', u'2017-09-29T11:55:25.724+02:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(978597, 8, 6, u'07ada7d5-eb41-4050-9687-0d3f918c00ce', 6955459, 0, 249, 2, 1915040, u'elixform', u'2017-09-29T11:55:29.903+02:00', u'elixform', u'2017-09-29T11:55:31.850+02:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(1089712, 8, 6, u'1845de0b-9ac7-4aae-b765-078e1d73b0f6', 7751467, 0, 249, 2, 2136722, u'elixform', u'2017-12-01T10:32:34.995+01:00', u'elixform', u'2017-12-01T10:32:45.906+01:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(1090246, 8, 6, u'8fe93896-64b4-425e-a550-5e296982fed9', 7755275, 0, 249, 2, 2137790, u'elixform', u'2017-12-01T12:45:29.273+01:00', u'elixform', u'2017-12-01T12:45:31.655+01:00', None, u'registroPg', 0, u'PROTOCOLLO'), 
(1098508, 8, 6, u'98efbf7c-ba77-46cc-ab30-b0c91ef6ec20', 7805849, 0, 249, 2, 2154282, u'elixform', u'2017-12-05T11:17:22.434+01:00', u'elixform', u'2017-12-05T11:17:30.861+01:00', None, u'registroPg', 0, u'PROTOCOLLO')]"""