import argparse
import os
import mysql.connector
import json

from utils.query import DocerAnalyer, DocerBonify

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--docer-doc-num', action='store', help="Numero Documento Docer", required=False)
parser.add_argument('-p', '--prot-num', action='store', help="Numero Protocollo", required=False)
parser.add_argument('-y', '--year', action='store', help="Anno di Analisi", required=False, default=2020)
parser.add_argument('--debug', action='store_true', help="Debug", required=False, default=False)
parser.add_argument('--output', action='store', help="Output", required=False, default='console')
parser.add_argument('-e', '--ente', action='store', help="Ente", required=False, default='unione')
parser.add_argument('-b', '--bonify', action='store_true', help="Bonify", required=False, default='false')
parser.add_argument('--b-registry-numbers', action='store', help="List of ids to bonify registry", required=False, default=None)


args = parser.parse_args()
docer_doc_num = args.docer_doc_num
prot_num = args.prot_num
debug = args.debug
docer_data = {args.docer_doc_num: {}}

# if args.bonify:
#     bonify = DocerBonify()
#     bonify.bootstrap(alf_host=os.environ.get('ALFRESCO_DB_HOST'),
#                    alf_user=os.environ.get('ALFRESCO_DB_USER'),
#                    alf_password=os.environ.get('ALFRESCO_DB_PWD'),
#                    alf_database=os.environ.get('ALFRESCO_DB'))
#     # res = bonify.get_node_by_registro(registro='PROTOCOLLO', limit=2)
#     bonify.set_nodes_by_registro(registro='PROTOCOLLO', limit=0, doc_numbers='871280, 872257')
#     print(bonify.rs_node_id_by_registro)
#     bonify.bonify_registro(registro_from='PROTOCOLLO', registro_to='PROT')
#     exit(99)
if not args.docer_doc_num and not args.prot_num:
    print("Specificare almeno uno tra 'Numero Documento Docer' e/o 'Numero Protocollo")
    exit(1)
elif args.docer_doc_num and not args.prot_num:
    print("---> Inizio analisi per 'Numero Documento Docer': {}".format(args.docer_doc_num))
elif not args.docer_doc_num and args.prot_num:
    print("---> Inizio analisi per 'Protocollo': {}".format(args.prot_num))
    print("TODO ---> esco..")
    exit(100)
elif args.docer_doc_num and args.prot_num:
    print("---> Inizio analisi per 'Protocollo' e 'Numero Documento"
          " Docer': {} - {} ".format(args.prot_num, args.docer_doc_num))
    print("TODO ---> esco..")
    exit(101)


analyzer = DocerAnalyer(docer_doc_num=args.docer_doc_num,
                        prot_num=args.prot_num,
                        year=args.year,
                        debug=args.debug)
analyzer.bootstrap(alf_host=os.environ.get('ALFRESCO_DB_HOST'),
                   alf_user=os.environ.get('ALFRESCO_DB_USER'),
                   alf_password=os.environ.get('ALFRESCO_DB_PWD'),
                   alf_database=os.environ.get('ALFRESCO_DB'),
                   akro_dsn="{}/{}".format(os.environ.get('AKROPOLIS_DB_HOST'),
                                                 os.environ.get('AKROPOLIS_DBO_SERVICE')),
                   akro_user=os.environ.get('AKROPOLIS_DB_USER_{}'.format(args.ente.upper())),
                   akro_password=os.environ.get('AKROPOLIS_DB_PASSWORD_{}'.format(args.ente.upper())))

analyzer.set_properties_by_docer_number()
analyzer.set_store_url_principal()

if analyzer.is_docer_doc_principal():
    analyzer.set_allegati()
    analyzer.set_akro_data()
else:
    analyzer.set_properties_of_principal()

if args.output == 'json':
    print(json.dumps(analyzer.docer_data))
    exit(0)

# print(analyzer.docer_data)
print(analyzer.format_console_output())
exit(0)

