import requests
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pprint
import datetime

#Gerando Token

URL = 'https://api.userede.com.br/redelabs/oauth/token'
payload = {
    'grant_type': 'password',
    'username': '',
    'password': ''
}
token = requests.post(url= URL, auth=('', ''), data = payload)
access_token = (token.json())
refresh_token = (access_token['access_token'])


#Acessando API
codigo = ['11391570','90459725','16110072','88307352','17507456','86819399','89121341','89122739','89122860','89296117','89527852',
'22265376','89122828','22265554','89122895','27040860','83061070','83682023','83725083','30565103','82192049','87084341','87084813','87085453','87085640',
'87085836','87086077','87255685','36317683','87249448','27745058','37519948','62252780','85182940','86572555','38038927','85291218','85291390','41411765',
'87610221','42140927','77947339','85182800','45797625','89720253','46708367','82789843','85291510','85291595','48315788','82120900','49598929','82070326',
'87560399','87560445','87560542','87560720','87560798','87560836','87561026','87561077','87561280','87561450','87561697','87561735','87561875','87561948',
'87562057','87562260','87562448','87562502','89721306','89721624','89825969','89826132','50719564','86819330','92067662','92068065','50748963','84451076',
'86933000','86933124','87323460','87323800','87323940','87324075','87324334','87324512','87324709','87324865','72528710','72528842','73046639','73046795',
'82051402','82068976','82069140','82069255','82107157','83044434','83044515','85677582','85677655','51256800','90523229','90524195','90525124','90525663',
'56924542','86825267','64141900','85181692','85181765','85183032','85183121','85207152','64193780','89122992','65434781','84688220','65617665',
'90520815','68830734','90418115','90457790','90459563','70641498','82070687','71515542','82122199','72239930','87406896','78587174','89682394','79426816',
'85732206','82033978','82034290','82103780','82103909','82034508','82070547','82050309','82106819','72528648','82050830','82070814','82121010','83694463',
'84232544','82105170','82122083','82034141','82069344','82120781','83046488','87671247','87671263','82034664','82104220','82121141','83222650','83222936',
'84547723','83241051','83397507','83711244','83711376','88255603','90836111','79196101','83348522','85181714','85181781','85182761','85183296','85183431',
'85183490','85182664','85183334','85181811','85182621','85182842','85182893','85192902','85192945','35344229','85207470','85310158','85324205','85448885',
'85449245','85633739','87413655','85893781','85893943','85964298','89121449','86933191','87407604','87084457','87413663','87085089','87085470','87125846',
'87345919','87346192','87346109','87346435','92057276','92057748','92236472','92237096','87455536','87554160','87559854','87491770','87491818','87952971',
'87983087','87502674','87502887','87920549','87921057','88206238','88206289','88802310','89682262','88937879','88953513','89720067','89808584','89153359',
'89153510','89682556','89682653','89682831','89682904','89728068','89728114','89728165','89728173','90325850','90326334','90326628','90326709','90383974',
'90435885','90515722','90519400','82107440','87629704','90918509','91093864']
pagetoken = ''
resultado = dict()
df_dataset = pd.DataFrame()
index = ''
list_of_dict = []
data = datetime.date.today() - datetime.timedelta(days=1)

for codigos in codigo:
    URL = f'https://api.userede.com.br/redelabs/merchant-statement/v1/sales?parentCompanyNumber={codigos}&subsidiaries={codigos}&startDate={data}&endDate={data}&size=100&pageKey={pagetoken}'
    headers = {'Authorization' : f'Bearer {refresh_token}'}
    req = requests.get(url= URL, headers= headers)
    dataset = req.json()
    resultado.update({codigos: dataset})
    dataset = resultado[f'{codigos}']['content']['transactions']
    
    
    try:
        pagetoken = (resultado[f'{codigos}']['cursor']['nextKey'])
    except:
        pagetoken = ''    
    
        
    for i in range(len(dataset)):
        df_vendas = {}
        df_vendas['codigo_tipo_captura'] = dataset[i]['captureTypeCode']
        df_vendas['nsu'] = dataset[i]['nsu']
        df_vendas['data_movimentacao'] = dataset[i]['movementDate']
        try:
            df_vendas['numero_pedido'] = dataset[i]['orderNumber']
        except:
            df_vendas['numero_pedido'] = ''
        df_vendas['tokenized'] = dataset[i]['tokenized']
        df_vendas['hora_venda'] = dataset[i]['saleHour']
        df_vendas['quantidade_parcelas'] = dataset[i]['installmentQuantity']
        df_vendas['taxa_total'] = dataset[i]['feeTotal']
        df_vendas['valor_liquido'] = dataset[i]['netAmount']
        try:
            df_vendas['tid'] = dataset[i]['tid']
        except:
            df_vendas['tid'] = ''
        df_vendas['tipo_dispositivo'] = dataset[i]['deviceType']
        df_vendas['valor_mdr'] = dataset[i]['mdrAmount']
        df_vendas['status'] = dataset[i]['status']
        df_vendas['data_venda'] = dataset[i]['saleDate']
        df_vendas['flex'] = dataset[i]['flex']
        df_vendas['taxa_flex'] = dataset[i]['flexFee']
        df_vendas['valor'] = dataset[i]['amount']
        df_vendas['numero_venda'] = dataset[i]['saleSummaryNumber']
        df_vendas['valor_flex'] = dataset[i]['flexAmount']
        df_vendas['valor_taxa_embarque'] = dataset[i]['boardingFeeAmount']
        df_vendas['codigo_autorizacao'] = dataset[i]['authorizationCode']
        df_vendas['tipo_captura'] = dataset[i]['captureType']
        try:
            df_vendas['codigo_maquininha'] = dataset[i]['divice']
        except:
            df_vendas['codigo_maquininha'] = ''
        try:
            df_vendas['numero_cartao'] = dataset[i]['cardNumber']
        except:
            df_vendas['numero_cartao'] = ''
        df_vendas['bandeira_cartao'] = dataset[i]['brandCode']
        df_vendas['taxa_mdr'] = dataset[i]['mdrFee']
        df_vendas['id_empresa'] = dataset[i]['merchant']['companyNumber']
        df_vendas['modality'] = dataset[i]['modality']['type']
        try:
            df_vendas['str_codigo_autorizacao'] = dataset[i]['strAuthorizationCode']
        except:
            df_vendas['str_codigo_autorizacao'] = ''
        try:
            df_vendas['ard'] = dataset[i]['ard']
        except:
            df_vendas['ard'] = ''
        list_of_dict.append(df_vendas)
        df_vendas = pd.DataFrame(list_of_dict)
    #df_dataset = df_dataset.append(df_vendas)

    while(pagetoken != ''):
        URL = f'https://api.userede.com.br/redelabs/merchant-statement/v1/sales?parentCompanyNumber={codigos}&subsidiaries={codigos}&startDate={data}&endDate={data}&size=100&pageKey={pagetoken}'
        headers = {'Authorization' : f'Bearer {refresh_token}'}
        req = requests.get(url= URL, headers= headers)
        dataset = req.json()
        resultado.update({codigos: dataset})
        dataset = resultado[f'{codigos}']['content']['transactions']


        try:
            pagetoken = (resultado[f'{codigos}']['cursor']['nextKey'])
        except:
            pagetoken = ''    


        for i in range(len(dataset)):
            df_vendas = {}
            df_vendas['codigo_tipo_captura'] = dataset[i]['captureTypeCode']
            df_vendas['nsu'] = dataset[i]['nsu']
            df_vendas['data_movimentacao'] = dataset[i]['movementDate']
            try:
                df_vendas['numero_pedido'] = dataset[i]['orderNumber']
            except:
                df_vendas['numero_pedido'] = ''
            df_vendas['tokenized'] = dataset[i]['tokenized']
            df_vendas['hora_venda'] = dataset[i]['saleHour']
            df_vendas['quantidade_parcelas'] = dataset[i]['installmentQuantity']
            df_vendas['taxa_total'] = dataset[i]['feeTotal']
            df_vendas['valor_liquido'] = dataset[i]['netAmount']
            try:
                df_vendas['tid'] = dataset[i]['tid']
            except:
                df_vendas['tid'] = ''
            df_vendas['tipo_dispositivo'] = dataset[i]['deviceType']
            df_vendas['valor_mdr'] = dataset[i]['mdrAmount']
            df_vendas['status'] = dataset[i]['status']
            df_vendas['data_venda'] = dataset[i]['saleDate']
            df_vendas['flex'] = dataset[i]['flex']
            df_vendas['taxa_flex'] = dataset[i]['flexFee']
            df_vendas['valor'] = dataset[i]['amount']
            df_vendas['numero_venda'] = dataset[i]['saleSummaryNumber']
            df_vendas['valor_flex'] = dataset[i]['flexAmount']
            df_vendas['valor_taxa_embarque'] = dataset[i]['boardingFeeAmount']
            df_vendas['codigo_autorizacao'] = dataset[i]['authorizationCode']
            df_vendas['tipo_captura'] = dataset[i]['captureType']
            try:
                df_vendas['codigo_maquininha'] = dataset[i]['divice']
            except:
                df_vendas['codigo_maquininha'] = ''
            try:
                df_vendas['numero_cartao'] = dataset[i]['cardNumber']
            except:
                df_vendas['numero_cartao'] = ''
            df_vendas['bandeira_cartao'] = dataset[i]['brandCode']
            df_vendas['taxa_mdr'] = dataset[i]['mdrFee']
            df_vendas['id_empresa'] = dataset[i]['merchant']['companyNumber']
            df_vendas['modality'] = dataset[i]['modality']['type']
            try:
                df_vendas['str_codigo_autorizacao'] = dataset[i]['strAuthorizationCode']
            except:
                df_vendas['str_codigo_autorizacao'] = ''
            try:
                df_vendas['ard'] = dataset[i]['ard']
            except:
                df_vendas['ard'] = ''
            list_of_dict.append(df_vendas)
            df_vendas = pd.DataFrame(list_of_dict)

#Tratando a coluna Index
df_vendas = df_vendas.reset_index(drop=True)


#Conexão com o Banco de Dados
conn = psycopg2.connect(database="",
                        user='postgres', password='', 
                        host='localhost', port='5432')

# Função para inserir dados no banco
def inserir_db(sql):
    con = conn
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()
    
    
#Conversão do DF para str
for col in df_vendas.columns:
    df_vendas[col] = df_vendas[col].apply(str)
    
#Inserindo dados no Banco de Dados
for i in df_vendas.index:
    sql = """
    INSERT into vendas_rede (codigo_tipo_captura,nsu,data_movimentacao,numero_pedido,tokenized,hora_venda,quantidade_parcelas,taxa_total,valor_liquido,tid,tipo_dispositivo,valor_mdr,status,data_venda,flex,taxa_flex,valor,numero_venda,valor_flex,valor_taxa_embarque,codigo_autorizacao,tipo_captura,codigo_maquininha,numero_cartao,bandeira_cartao,taxa_mdr,id_empresa,modality,str_codigo_autorizacao,ard)
    values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
    """ %(df_vendas['codigo_tipo_captura'][i], df_vendas['nsu'][i], df_vendas['data_movimentacao'][i], df_vendas['numero_pedido'][i],df_vendas['tokenized'][i], df_vendas['hora_venda'][i], df_vendas['quantidade_parcelas'][i],df_vendas['taxa_total'][i],df_vendas['valor_liquido'][i],df_vendas['tid'][i],df_vendas['tipo_dispositivo'][i],df_vendas['valor_mdr'][i],df_vendas['status'][i],df_vendas['data_venda'][i],df_vendas['flex'][i],df_vendas['taxa_flex'][i],df_vendas['valor'][i],df_vendas['numero_venda'][i],df_vendas['valor_flex'][i],df_vendas['valor_taxa_embarque'][i],df_vendas['codigo_autorizacao'][i],df_vendas['tipo_captura'][i],df_vendas['codigo_maquininha'][i],df_vendas['numero_cartao'][i],df_vendas['bandeira_cartao'][i],df_vendas['taxa_mdr'][i],df_vendas['id_empresa'][i],df_vendas['modality'][i],df_vendas['str_codigo_autorizacao'][i],df_vendas['ard'][i])
    inserir_db(sql)