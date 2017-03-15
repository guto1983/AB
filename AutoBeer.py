import ast
import email
import getpass, imaplib
import os
import sys
import datetime
import time
import sqlite3
import threading


#####################################################
## Declara as variaveis globais
#####################################################

flagGravaBD = False
flagConsultaUltimaDataBD = False
flagEmailAtualizadoDia = False
countEventos = 0

eventoRecebido = {}
dataHoraRecebida = {}
valorRecebido = {}
emailRecebido = {}
trnIdRecebido = {}
latLongRecebida = {}


#####################################################
## Classe que inicia a thread de tratativa de BD
#####################################################

class threadTrataBD(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.name = 'TrataBDThread'
		
	def run(self):
		TrataBD()


#####################################################
## Classe que inicia a thread de recepcao de email
#####################################################

class threadCapturaEmail(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.name = 'CapturaEmailThread'
		
	def run(self):
		CapturaEmail()



#######################################################
## Funcao que trata o Banco de dados
#######################################################		

def TrataBD():
	global flagGravaBD
	global flagConsultaUltimaDataBD
	global ultimaDataBD
	global diaConsultaEmail
	global countEventos
	
	global eventoRecebido
	global dataHoraRecebida
	global valorRecebido
	global emailRecebido
	global trnIdRecebido
	
	valorRecebidoTemp = {}
	saldoAtual = {}
	
	print('Inicia thread de tratamento do BD')
	
	while(True):
		# Verifica se precisa gravar no BD
		if(flagGravaBD == True):
			print('\nGrava no BD...')
			
			# Cria o banco
			connection = sqlite3.connect('AutoBeer.db')
			c = connection.cursor()

			# Cria a tabela de relatorio geral
			c.execute('''CREATE TABLE IF NOT EXISTS Cadastro(Email varchar, UltimoEvento varchar, ValorOperacao varchar, Saldo varchar, UltimaAtualizacao varchar, UltimoIdTRN varchar)''')

			# Varre todos os eventos a serem gravados
			for countGravaEvento in range(countEventos):
				# Verifica se e um evento de carga
				if(eventoRecebido[countGravaEvento] == 'Carga'):
					print('\nGravando evento de carga...')
					# Seleciona o campo de saldo
					c.execute('''select Saldo from Cadastro where Email = "%s"''' % (emailRecebido[countGravaEvento]))
					
					# Carrega o valor de ultimo saldo
					ultimoSaldo = c.fetchone()
					
					# Faz o parse do valor
					valorRecebidoTemp[countGravaEvento] = str(valorRecebido[countGravaEvento].replace(',', '.'))
					
					# Verifica se e um cliente novo
					if(ultimoSaldo == None):
						print('Cadastra cliente')
						print('Gravando data/hora: %s' % dataHoraRecebida[countGravaEvento])
						print('Gravando valor: %s' % valorRecebidoTemp[countGravaEvento])
						print('Gravando email: %s' % emailRecebido[countGravaEvento])
						print('Gravando ID transacao: %s' % trnIdRecebido[countGravaEvento])
						
						# Insere no BD
						c.execute('''INSERT INTO Cadastro values(?,?,?,?,?,?)''', (emailRecebido[countGravaEvento], eventoRecebido[countGravaEvento], valorRecebidoTemp[countGravaEvento], valorRecebidoTemp[countGravaEvento], dataHoraRecebida[countGravaEvento], trnIdRecebido[countGravaEvento]))
					
					else:
						print('Atualiza cliente')
						print('Gravando data/hora: %s' % dataHoraRecebida[countGravaEvento])
						print('Gravando valor: %s' % valorRecebidoTemp[countGravaEvento])
						print('Gravando email: %s' % emailRecebido[countGravaEvento])
						print('Gravando ID transacao: %s' % trnIdRecebido[countGravaEvento])
						
						# Atualiza o saldo
						saldoAtual[countGravaEvento] = str(ast.literal_eval(ultimoSaldo[0]) + ast.literal_eval(valorRecebidoTemp[countGravaEvento]))
						
						# Atualiza no BD
						c.execute('''UPDATE Cadastro SET UltimoEvento = ?, ValorOperacao = ?, Saldo = ?, UltimaAtualizacao = ?, UltimoIdTRN = ? where Email = "%s"''' % (emailRecebido[countGravaEvento]), (eventoRecebido[countGravaEvento], valorRecebidoTemp[countGravaEvento], saldoAtual[countGravaEvento], dataHoraRecebida[countGravaEvento], trnIdRecebido[countGravaEvento]))

					# Commit
					connection.commit()
					
				# Verifica se e um evento de carga
				elif(eventoRecebido[countGravaEvento] == 'Pedido'):
					print('\nGravando evento de pedido...')
					# Seleciona o campo de saldo
					c.execute('''select Saldo from Cadastro where Email = "%s"''' % (emailRecebido[countGravaEvento]))
					
					# Carrega o valor de ultimo saldo
					ultimoSaldo = c.fetchone()
					
					# Faz o parse do valor
					valorRecebidoTemp[countGravaEvento] = str(valorRecebido[countGravaEvento].replace(',', '.'))
					
					# Verifica se e um cliente novo
					if((ultimoSaldo == None) or (ast.literal_eval(ultimoSaldo[0]) < ast.literal_eval(valorRecebidoTemp[countGravaEvento]))):
						print('Saldo nao disponivel...')
					
					else:
						print('Atualiza cliente')
						print('Gravando data/hora: %s' % dataHoraRecebida[countGravaEvento])
						print('Gravando valor: %s' % valorRecebidoTemp[countGravaEvento])
						print('Gravando email: %s' % emailRecebido[countGravaEvento])
						print('Gravando ID transacao: %s' % trnIdRecebido[countGravaEvento])
						
						# Atualiza o saldo
						saldoAtual[countGravaEvento] = str(ast.literal_eval(ultimoSaldo[0]) - ast.literal_eval(valorRecebidoTemp[countGravaEvento]))
						
						# Atualiza no BD
						c.execute('''UPDATE Cadastro SET UltimoEvento = ?, ValorOperacao = ?, Saldo = ?, UltimaAtualizacao = ?, UltimoIdTRN = ? where Email = "%s"''' % (emailRecebido[countGravaEvento]), (eventoRecebido[countGravaEvento], valorRecebidoTemp[countGravaEvento], saldoAtual[countGravaEvento], dataHoraRecebida[countGravaEvento], trnIdRecebido[countGravaEvento]))

					# Commit
					connection.commit()
					
			# Encerra a conexao
			connection.close()
			
			# Zera o contador
			countEventos = 0
			
			# Reseta a flag
			flagGravaBD = False
			
		if(flagConsultaUltimaDataBD == True):
			# Cria o banco
			connection = sqlite3.connect('AutoBeer.db')
			c = connection.cursor()

			# Cria a tabela de relatorio geral
			c.execute('''CREATE TABLE IF NOT EXISTS Cadastro (Email varchar, UltimoEvento varchar, ValorOperacao varchar, Saldo varchar, UltimaAtualizacao varchar, UltimoIdTRN varchar)''')
			
			# Seleciona o campo de ultima data/hora de atualizacao
			c.execute('''select UltimaAtualizacao from Cadastro order by UltimaAtualizacao''')
			
			# Carrega o valor de ultimo saldo
			ultimaDataBD = c.fetchall()
			
			# Verifica se o BD esta vazio
			if(len(ultimaDataBD) == 0):
				# Assume uma data incial
				ultimaDataBD = '01-Jan-2017 00:00:00'
				diaConsultaEmail = ultimaDataBD[0:11]
				
			else:
				# Carrega a ultima data do BD
				ultimaDataBD = ultimaDataBD[len(ultimaDataBD)-1]
				ultimaDataBD = ultimaDataBD[0]
				diaConsultaEmail = datetime.datetime.now().strftime("%d-%b-%Y")
			
			print('\n[%s] Ultima data BD - %s' % (datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), ultimaDataBD))
			
			# Encerra a conexao
			connection.close()
			
			# Reseta a flag
			flagConsultaUltimaDataBD = False


#######################################################
## Funcao que trata a recepcao de emails
#######################################################		

def CapturaEmail():
	global flagGravaBD
	global flagConsultaUltimaDataBD
	global ultimaDataBD
	global diaConsultaEmail
	global countEventos
	
	global eventoRecebido
	global dataHoraRecebida
	global valorRecebido
	global emailRecebido
	global trnIdRecebido
	global latLongRecebida
	
	print('Inicia thread de captura de email')
	
	# Reseta a flag
	flagCorpoEmail = False
	
	# Zera o contador
	countEventos = 0
	
	detach_dir = '.'
	'''if 'attachments1' not in os.listdir(detach_dir):
		os.mkdir('attachments1')'''

	# Usuario e senha do email a conectar
	userName = 'autobeer2017'
	passwd = 'B-yxxSY3_z'

	#try:
	imapSession = imaplib.IMAP4_SSL('imap.gmail.com')
	typ, accountDetails = imapSession.login(userName, passwd)
	
	# Verifica ocorreu erro na conexao da conta do email
	if(typ != 'OK'):
		print('Nao conseguiu conectar na conta de email!')
		raise
	
	# Loop
	while(True):
		# Aguarda 2 segundos
		time.sleep(2)
		
		# Verifica se nao tem eventos para consumo do BD na fila
		if(countEventos == 0):
			# Aguarda a ultima data do BD
			flagConsultaUltimaDataBD = True
			while(flagConsultaUltimaDataBD == True):
				continue

			print('Verificando emails a partir do dia %s...' % diaConsultaEmail)
			
			# Seleciona os filtros para busca dos emails
			#imapSession.select('[Gmail]/All Mail')
			imapSession.select('INBOX', True)
			#typ, data = imapSession.search(None, 'FROM', '"Augusto Thadeu Freitas Spedaletti"')
			#typ, data = imapSession.search(None, 'ALL')
			typ, data = imapSession.search(None, '(SINCE "%s")' % diaConsultaEmail)
			
			# Verifica se ocorreu erro na busca dos emails
			if typ != 'OK':
				print('Erro na busca emails!')
				raise
				
			# Iterating over all emails
			for msgId in data[0].split():
				typ, messageParts = imapSession.fetch(msgId, '(RFC822)')
				
				# Verifica se ocorreu erro na captura do conteudo do email
				if typ != 'OK':
					print('Erro na captura do conteudo do email!')
					raise
						
				# Carrega o conteudo do email
				emailBody = messageParts[0][1]
				mail = email.message_from_bytes(emailBody)
				
				# Converte a ultima data/hora do BD em timestamp para comparacao
				d = datetime.datetime.strptime(ultimaDataBD, '%d-%b-%Y %H:%M:%S')
				ultimaDataBDTimestamp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
				
				# Converte a ultima data/hora do email em timestamp para comparacao
				dataHoraRecebidaTempTimestamp = mail['date'].split()
				dataHoraRecebidaTimestamp = dataHoraRecebidaTempTimestamp[1] + '-' + dataHoraRecebidaTempTimestamp[2] + '-' + dataHoraRecebidaTempTimestamp[3] + ' ' + dataHoraRecebidaTempTimestamp[4]
				d = datetime.datetime.strptime(dataHoraRecebidaTimestamp, '%d-%b-%Y %H:%M:%S')
				dataHoraRecebidaTimestamp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
				
				# Verifica se o email e mais novo que o ultimo evento do BD
				if(dataHoraRecebidaTimestamp > ultimaDataBDTimestamp):
					for part in mail.walk():
						# Verifica se e um email de carga
						if('received a payment' in mail['subject']):
							if part.get_content_type() == 'text/plain':
								# Salva como evento de carga
								eventoRecebido[countEventos] = 'Carga'
								
								# Faz o parse da data/hora do email para gravacao no BD
								dataHoraRecebidaTemp = mail['date'].split()
								dataHoraRecebida[countEventos] = dataHoraRecebidaTemp[1] + '-' + dataHoraRecebidaTemp[2] + '-' + dataHoraRecebidaTemp[3] + ' ' + dataHoraRecebidaTemp[4]
								
								# Zera a flag e carrega o payload do email
								flagCorpoEmail = False
								body = part.get_payload(decode=False)
								
								# Separa o texto do conteudo do email
								textoRecebido = body.split()
								
								# Varre todas as palavras
								for posicaoPalavra in range(len(textoRecebido)):
									# Verifica se encontrou o valor do credito
									if((textoRecebido[posicaoPalavra] == 'Payment') and (textoRecebido[posicaoPalavra+1] == 'received')):
										# Salva o valor
										valorRecebido[countEventos] = textoRecebido[posicaoPalavra+3]
									
									# Verifica se encontrou o email que realizou o credito
									elif((textoRecebido[posicaoPalavra] == '[image:') and (textoRecebido[posicaoPalavra+1] == 'quote]') and (flagCorpoEmail == False)):
										# Seta a flag e salva o valor
										flagCorpoEmail = True
										emailRecebido[countEventos] = textoRecebido[posicaoPalavra+2]
									
									# Verifica se encontrou o ID da transacao do credito	
									elif((textoRecebido[posicaoPalavra] == 'Transaction') and (textoRecebido[posicaoPalavra+1] == 'ID:')):
										# Salva o ID da transacao
										trnIdRecebido[countEventos] = textoRecebido[posicaoPalavra+2]
					
								# Incrementa o contador
								countEventos += 1

						elif('Button pressed' in mail['subject']):
							if part.get_content_type() == 'text/plain':
								# Salva como evento de pedido
								eventoRecebido[countEventos] = 'Pedido'
								
								# Faz o parse da data/hora do email para gravacao no BD
								dataHoraRecebidaTemp = mail['date'].split()
								dataHoraRecebida[countEventos] = dataHoraRecebidaTemp[1] + '-' + dataHoraRecebidaTemp[2] + '-' + dataHoraRecebidaTemp[3] + ' ' + dataHoraRecebidaTemp[4]
									
								# Zera a flag e carrega o payload do email
								flagCorpoEmail = False
								body = part.get_payload(decode=False)
								
								# Separa o texto do conteudo do email
								textoRecebido = body.split()
								
								# Varre todas as palavras
								for posicaoPalavra in range(len(textoRecebido)):
									# Verifica se encontrou no email a LAT/LONG
									if(('-' in textoRecebido[posicaoPalavra]) and (flagCorpoEmail == False)):
										# Seta a flag e salva o valor
										flagCorpoEmail = True
										latLongRecebida[countEventos] = textoRecebido[posicaoPalavra] + ' ' + textoRecebido[posicaoPalavra+1]
									
								# Salva o valor (inicialmente somente R$7,00)
								valorRecebido[countEventos] = '7,00'
									
								# Salva o remetente do pedido
								emailRecebido[countEventos] = mail['from']
								
								# Salva o ID da transacao
								trnIdRecebido[countEventos] = ''
								
								print('Chegou %s de %s no valor de R$%s para %s' % (eventoRecebido[countEventos], emailRecebido[countEventos], valorRecebido[countEventos], latLongRecebida[countEventos]))
								
								# Incrementa o contador
								countEventos += 1
												
						'''if bool(fileName):
							filePath = os.path.join(detach_dir, 'attachments1', fileName)
							if not os.path.isfile(filePath) :
								print('\nAnexos:')
								fp = open(filePath, 'wb')
								fp.write(part.get_payload(decode=True))
								fp.close()'''
								
			# Verifica se existe(m) evento(s) para gtravar no BD
			if(countEventos > 0):
				flagGravaBD = True
	
	# Encerra a sessao
	imapSession.close()
	imapSession.logout()

	'''except :
		print('Erro geral na tratativa de recepcao de email!')'''


##########################################
## Main
##########################################

print('\n[%s] AutoBeer 2017\n' % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

# Inicia as threads
CapturaEmailThread = threadCapturaEmail()
CapturaEmailThread.start()
TrataBDThread = threadTrataBD()
TrataBDThread.start()

