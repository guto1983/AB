import ast
import email
from email.utils import parsedate_tz, mktime_tz, formatdate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import getpass, imaplib
import smtplib
import os
import sys
import datetime
import time
import sqlite3
import threading
from geopy.distance import great_circle
import RPi.GPIO as gpio


#####################################################
## Declara as definicoes
#####################################################

# Geral
DISTANCIA_MAXIMA_ACEITAVEL = 30
TEMPO_MINIMO_ENVIO_EMAIL = 60
PULSOS_500ML = 100

# Pinagem
PINO_ACENDE_LED = 7
PINO_CONTROLE_BOMBA = 11
PINO_BOTAO = 13
PINO_SENSOR_FLUXO = 15

# Maquina de estado geral
ESTADO_INICIO = 0
VERIFICA_BOTAO = 1
VERIFICA_FLUXO = 2
ESTADO_FINAL = 3

# Maquina de estado do led
DESLIGA_LED = 0
LIGA_LED = 1
PISCA_LED = 2

# Maquina de estado da bomba
DESLIGA_BOMBA = 0
LIGA_BOMBA = 1

# Maquina de estado do botao
BOTAO_DESACIONADO = 0
BOTAO_ACIONADO = 1

# Maquina de estado do sensor de fluxo
PARA_CARGA = 0
LIBERA_CARGA = 1


#####################################################
## Declara as variaveis globais
#####################################################

flagGravaBD = False
flagConsultaUltimaDataBD = False
flagEmailAtualizadoDia = False
flagVerificaEmail = False
flagEnviaEmail = False
flagAcionaAutomacao = False
contadorPulsosSensorFluxo = 0
flagPulsoSensorFluxo = False

eventoRecebido = ''
dataHoraRecebida = ''
valorRecebido = ''
emailRecebido = ''
trnIdRecebido = ''
latLongRecebida = ''

emailEnviaMsg = ''
textoEnviaMsg = ''

idMaquina = ''
posicaoMaquina = ''


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
		
		
#####################################################
## Classe que inicia a thread de envio de email
#####################################################

class threadEnviaEmail(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.name = 'EnviaEmailThread'
		
	def run(self):
		EnviaEmail()
		
		
#####################################################
## Classe que inicia a thread de tratamento de pedido
#####################################################

class threadTrataPedido(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.name = 'TrataPedidoThread'
		
	def run(self):
		TrataPedido()


#######################################################
## Funcao que trata o Banco de dados
#######################################################		

def TrataBD():
	global flagGravaBD
	global flagConsultaUltimaDataBD
	global ultimaDataBD
	global diaConsultaEmail
	global flagVerificaEmail
	global flagEnviaEmail
	global flagAcionaAutomacao
	
	global eventoRecebido
	global dataHoraRecebida
	global valorRecebido
	global emailRecebido
	global trnIdRecebido
	global latLongRecebida
	
	global emailEnviaMsg
	global textoEnviaMsg
	
	global idMaquina
	global posicaoMaquina
	
	valorRecebidoTemp = {}
	saldoAtual = {}
	
	flagClienteProximoMaquina = False
	
	print('Inicia thread de tratamento do BD')
	
	# Cria o banco
	connection = sqlite3.connect('AutoBeer.db')
	c = connection.cursor()

	# Cria a tabela de Id da maquina
	c.execute('''CREATE TABLE IF NOT EXISTS InfosMaquina(Id varchar, Posicao varchar, UltimaAtualizacao varchar, Email varchar)''')
	
	# Seleciona o campo de Id da maquina
	c.execute('''select Id from InfosMaquina order by UltimaAtualizacao''')
	
	# Carrega o valor de Id da maquina
	idMaquina = c.fetchall()
	
	# Verifica se nao tem dados na tabela ainda
	if(len(idMaquina) == 0):
		# Solicita no teminal o cadastro de Id da maquina
		idMaquina = input("\nDigite o ID (numerico) desta maquina para cadastra-lo:\n\n")
		
		# Insere no BD
		c.execute('''INSERT INTO InfosMaquina values(?,"","","Cadastro")''', idMaquina)
		
		# Commit
		connection.commit()
	
	else:
		# Ajusta a string
		idMaquina = idMaquina[len(idMaquina)-1]
		idMaquina = idMaquina[0]
		
	print('\nMaquina batizada com o ID "%s"' % idMaquina)
		
	# Seleciona o campo de localizacao da maquina
	c.execute('''select Posicao from InfosMaquina order by UltimaAtualizacao''')
	
	# Carrega o valor da localizacao da maquina
	posicaoMaquina = c.fetchall()
	posicaoMaquina = posicaoMaquina[len(posicaoMaquina)-1]
	posicaoMaquina = posicaoMaquina[0]
	
	# Verifica se nao tem dados na posicao ainda
	if(posicaoMaquina == ''):
		print('Maquina sem localizacao GPS cadastrada ainda...\n')
		
	else:
		print('Maquina batizada com a localizacao "%s"\n' % posicaoMaquina)
	
	# Encerra a conexao
	connection.close()
	
	# Seta a flag que habilita a leitura de emails
	flagVerificaEmail = True
	
	# Loop
	while(True):
		# Verifica se precisa gravar no BD
		if(flagGravaBD == True):
			print('\nGrava no BD...')
			
			# Cria o banco
			connection = sqlite3.connect('AutoBeer.db')
			c = connection.cursor()

			# Cria a tabela de cadastro
			c.execute('''CREATE TABLE IF NOT EXISTS Cadastro(Email varchar, UltimoEvento varchar, ValorOperacao varchar, Saldo varchar, UltimaAtualizacao varchar, UltimoIdTRN varchar)''')

			# Verifica se e um evento de carga
			if(eventoRecebido == 'Carga'):
				print('\nGravando evento de carga...')
				# Seleciona o campo de saldo
				c.execute('''select Saldo from Cadastro where Email = "%s"''' % emailRecebido)
				
				# Carrega o valor de ultimo saldo
				ultimoSaldo = c.fetchone()
				
				# Faz o parse do valor
				valorRecebidoTemp = str(valorRecebido.replace(',', '.'))
				
				# Verifica se e um cliente novo
				if(ultimoSaldo == None):
					print('Cadastra cliente')
					print('Gravando data/hora: %s' % dataHoraRecebida)
					print('Gravando valor: %s' % valorRecebidoTemp)
					print('Gravando email: %s' % emailRecebido)
					print('Gravando ID transacao: %s' % trnIdRecebido)
					
					# Insere no BD
					c.execute('''INSERT INTO Cadastro values(?,?,?,?,?,?)''', (emailRecebido, eventoRecebido, valorRecebidoTemp, valorRecebidoTemp, dataHoraRecebida, trnIdRecebido))
				
				else:
					print('Atualiza cliente')
					print('Gravando data/hora: %s' % dataHoraRecebida)
					print('Gravando valor: %s' % valorRecebidoTemp)
					print('Gravando email: %s' % emailRecebido)
					print('Gravando ID transacao: %s' % trnIdRecebido)
					
					# Atualiza o saldo
					saldoAtual = str(ast.literal_eval(ultimoSaldo[0]) + ast.literal_eval(valorRecebidoTemp))
					
					# Atualiza no BD
					c.execute('''UPDATE Cadastro SET UltimoEvento = ?, ValorOperacao = ?, Saldo = ?, UltimaAtualizacao = ?, UltimoIdTRN = ? where Email = "%s"''' % (emailRecebido), (eventoRecebido, valorRecebidoTemp, saldoAtual, dataHoraRecebida, trnIdRecebido))

				# Commit
				connection.commit()
				
				# Seta a flag
				flagVerificaEmail = True

			# Verifica se e um evento de carga
			elif(eventoRecebido == 'Pedido'):
				print('\nGravando evento de pedido...')
				# Seleciona o campo de saldo
				c.execute('''select Saldo from Cadastro where Email = "%s"''' % emailRecebido)
				
				# Carrega o valor de ultimo saldo
				ultimoSaldo = c.fetchone()
				
				# Faz o parse do valor
				valorRecebidoTemp = str(valorRecebido.replace(',', '.'))
				
				# Verifica se nao e DUMP (cadastrado com limite, nao cadastrado ou sem limite)
				if(emailRecebido != 'DUMP'):
					# Verifica se e um cliente novo ou nao tem limite
					if((ultimoSaldo == None) or (ast.literal_eval(ultimoSaldo[0]) < ast.literal_eval(valorRecebidoTemp))):
						print('Saldo nao disponivel ou cliente nao cadastrado...')
						
						# Simula dump
						emailRecebido = 'DUMP'
						valorRecebidoTemp = '0.00'
						trnIdRecebido = ''
						
						# Seleciona o campo de saldo
						c.execute('''select Saldo from Cadastro where Email = "DUMP"''')
						
						# Carrega o valor de ultimo saldo
						ultimoSaldoTemp = c.fetchone()
						
						# Verifica se e um cliente novo
						if(ultimoSaldo == None):
							# Carrega valor default
							saldoAtual = '0.00'

						else:
							# Carrega o saldo atual
							saldoAtual = str(ast.literal_eval(ultimoSaldo[0]))
							
						# Verifica se e um DUMP novo
						if(ultimoSaldoTemp == None):
							# Carrega os valores default
							ultimoSaldo = None
							
						else:
							# Carrega os valores default
							ultimoSaldo = '0.00'

						# Seta a flag
						flagClienteProximoMaquina = True
						
						# Seta a flag
						flagVerificaEmail = True
					
					else:
						print('Atualiza cliente')
						print('Gravando data/hora: %s' % dataHoraRecebida)
						print('Gravando valor: %s' % valorRecebidoTemp)
						print('Gravando email: %s' % emailRecebido)
						print('Gravando ID transacao: %s' % trnIdRecebido)
						
						# Atualiza o saldo
						saldoAtual = str(ast.literal_eval(ultimoSaldo[0]) - ast.literal_eval(valorRecebidoTemp))
						
						# Atualiza no BD
						c.execute('''UPDATE Cadastro SET UltimoEvento = ?, ValorOperacao = ?, Saldo = ?, UltimaAtualizacao = ?, UltimoIdTRN = ? where Email = "%s"''' % (emailRecebido), (eventoRecebido, valorRecebidoTemp, saldoAtual, dataHoraRecebida, trnIdRecebido))

						# Commit
						connection.commit()
						
						# Carrega o texto para enviar o email
						textoEnviaMsg = 'Parabens, seu pedido de ' + dataHoraRecebida + ' foi processado com sucesso!\n\nSeu saldo disponivel e: R$ ' + saldoAtual
						
						# Carrega os timestamps para comparacao
						d = datetime.datetime.strptime(dataHoraRecebida, '%d-%b-%Y %H:%M:%S')
						dataHoraRecebidaTimestamp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
						
						d = datetime.datetime.now().strftime('%d-%b-%Y %H:%M:%S')
						dataHoraAtualTimestamp = time.mktime(datetime.datetime.strptime(d, '%d-%b-%Y %H:%M:%S').timetuple())
						
						# Verifica se deve tomar acao (envio de email e liberacao da automacao) ou somente gravar no BD
						if((dataHoraAtualTimestamp - dataHoraRecebidaTimestamp) < TEMPO_MINIMO_ENVIO_EMAIL):
							# Seta a flag de Envio de email
							flagEnviaEmail = True
							
							# Seta a flag de tratamento da automacao
							flagAcionaAutomacao = True
							
							# Reseta a flag
							flagVerificaEmail = False
							
						else:
							# Seta a flag
							flagVerificaEmail = True

				# Verifica se e DUMP (fora da zona de pedido da maquina)
				if(emailRecebido == 'DUMP'):
					# Verifica se e um cliente novo
					if(ultimoSaldo == None):
						print('Cadastra DUMP')
						print('Gravando data/hora: %s' % dataHoraRecebida)
						print('Gravando valor: %s' % valorRecebidoTemp)
						print('Gravando email: %s' % emailRecebido)
						print('Gravando ID transacao: %s' % trnIdRecebido)
						
						# Insere no BD
						c.execute('''INSERT INTO Cadastro values(?,?,?,?,?,?)''', (emailRecebido, eventoRecebido, valorRecebidoTemp, valorRecebidoTemp, dataHoraRecebida, trnIdRecebido))

					else:
						print('Atualiza DUMP')
						print('Gravando data/hora: %s' % dataHoraRecebida)
						print('Gravando valor: %s' % valorRecebidoTemp)
						print('Gravando email: %s' % emailRecebido)
						print('Gravando ID transacao: %s' % trnIdRecebido)
						
						# Atualiza no BD
						c.execute('''UPDATE Cadastro SET UltimoEvento = ?, ValorOperacao = ?, Saldo = "0.00", UltimaAtualizacao = ?, UltimoIdTRN = ? where Email = "%s"''' % (emailRecebido), (eventoRecebido, valorRecebidoTemp, dataHoraRecebida, trnIdRecebido))

					# Commit
					connection.commit()
					
					# Verifica se deve enviar a msg de cliente nao cadastrado ou sem limite
					if(flagClienteProximoMaquina == True):
						# Reseta a flag
						flagClienteProximoMaquina = False
						
						# Carrega o texto para enviar o email
						textoEnviaMsg = 'Desculpem-nos, mas nao foi possivel atender ao pedido solicitado em ' + dataHoraRecebida + '!\n\nSeu saldo disponivel e: R$ ' + saldoAtual
						
					else:
						# Carrega o texto para enviar o email
						textoEnviaMsg = 'Desculpem-nos, mas nao foi possivel atender ao pedido solicitado em ' + dataHoraRecebida + '!\n\nSeu pedido foi realizado fora da distancia minima (' + str(DISTANCIA_MAXIMA_ACEITAVEL) + ' metros) de alguma maquina'

					# Carrega os timestamps para comparacao
					d = datetime.datetime.strptime(dataHoraRecebida, '%d-%b-%Y %H:%M:%S')
					dataHoraRecebidaTimestamp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
					
					d = datetime.datetime.now().strftime('%d-%b-%Y %H:%M:%S')
					dataHoraAtualTimestamp = time.mktime(datetime.datetime.strptime(d, '%d-%b-%Y %H:%M:%S').timetuple())

					# Verifica se deve tomar acao (envio de email) ou somente gravar no BD
					if((dataHoraAtualTimestamp - dataHoraRecebidaTimestamp) < TEMPO_MINIMO_ENVIO_EMAIL):
						# Seta a flag de Envio de email
						flagEnviaEmail = True
						
					# Seta a flag
					flagVerificaEmail = True
						
			# Verifica se e um evento de gravacao de posicao de maquina
			elif(eventoRecebido == 'GravaPosicao'):
				print('\nGravando localizacao da maquina...')
				
				print('Cadastra localizacao de maquina')
				print('Gravando id da maquina: %s' % idMaquina)
				print('Gravando localizacao da maquina: %s' % latLongRecebida)
				print('Gravando data/hora: %s' % dataHoraRecebida)
				print('Gravando email: %s' % emailRecebido)
				
				# Insere no BD
				c.execute('''INSERT INTO InfosMaquina values(?,?,?,?)''', (idMaquina, latLongRecebida, dataHoraRecebida, emailRecebido))
				
				# Commit
				connection.commit()
				
				# Grava a posicao na variavel global
				posicaoMaquina = latLongRecebida
				
				# Seta a flag
				flagVerificaEmail = True

			# Encerra a conexao
			connection.close()
			
			# Reseta a flag
			flagGravaBD = False
			
		if(flagConsultaUltimaDataBD == True):
			# Cria o banco
			connection = sqlite3.connect('AutoBeer.db')
			c = connection.cursor()

			# Cria a tabela de cadastro
			c.execute('''CREATE TABLE IF NOT EXISTS Cadastro (Email varchar, UltimoEvento varchar, ValorOperacao varchar, Saldo varchar, UltimaAtualizacao varchar, UltimoIdTRN varchar)''')
			
			# Seleciona o campo de ultima data/hora de atualizacao
			c.execute('''select UltimaAtualizacao from Cadastro order by UltimaAtualizacao''')
			
			# Carrega o valor de ultima data/hora
			ultimaDataBD = c.fetchall()
			
			# Verifica se o BD esta vazio
			if(len(ultimaDataBD) == 0):
				# Assume uma data incial
				ultimaDataBD = '01-Jan-2017 00:00:00'
				
				# Converte a ultima data/hora do BD em timestamp para comparacao
				ultimaDataBDTimestampTemp = time.mktime(datetime.datetime.strptime(ultimaDataBD, '%d-%b-%Y %H:%M:%S').timetuple())
				
			else:
				# Carrega a ultima data do BD
				ultimaDataBD = ultimaDataBD[len(ultimaDataBD)-1]
				ultimaDataBD = ultimaDataBD[0]
				
				# Converte a ultima data/hora do BD em timestamp para comparacao
				d = datetime.datetime.strptime(ultimaDataBD, '%d-%b-%Y %H:%M:%S')
				ultimaDataBDTimestampTemp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
				
			# Cria a tabela de Id da maquina
			c.execute('''CREATE TABLE IF NOT EXISTS InfosMaquina(Id varchar, Posicao varchar, UltimaAtualizacao varchar, Email varchar)''')
			
			# Seleciona o campo de ultima data/hora de atualizacao
			c.execute('''select UltimaAtualizacao from InfosMaquina order by UltimaAtualizacao''')
			
			# Carrega o valor de ultima data/hora
			ultimaDataBDTab2 = c.fetchall()
			
			# Verifica se o BD esta vazio
			if(len(ultimaDataBDTab2) == 1):
				# Assume uma data incial
				ultimaDataBDTab2 = '01-Jan-2017 00:00:00'
				
				# Converte a ultima data/hora do BD em timestamp para comparacao
				ultimaDataBDTimestampTempTab2 = time.mktime(datetime.datetime.strptime(ultimaDataBDTab2, '%d-%b-%Y %H:%M:%S').timetuple())
				
			else:
				# Carrega a ultima data do BD
				ultimaDataBDTab2 = ultimaDataBDTab2[len(ultimaDataBDTab2)-1]
				ultimaDataBDTab2 = ultimaDataBDTab2[0]
				
				# Converte a ultima data/hora do BD em timestamp para comparacao
				d = datetime.datetime.strptime(ultimaDataBDTab2, '%d-%b-%Y %H:%M:%S')
				ultimaDataBDTimestampTempTab2 = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()

			# Verifica se a ultima data/hora da tabela 2 e maior que da tabela 1
			if(ultimaDataBDTimestampTemp < ultimaDataBDTimestampTempTab2):
				# Carrega o valor de ultima data/hora
				ultimaDataBD = ultimaDataBDTab2
				
			# Verifica se ainda nao tem registros salvos
			if(ultimaDataBD == '01-Jan-2017 00:00:00'):
				# Salva a data inicial
				diaConsultaEmail = ultimaDataBD[0:11]
				
			else:
				# Salva a data atual
				diaConsultaEmail = datetime.datetime.now().strftime("%d-%b-%Y")
			
			print('\n[%s] Ultima data BD: %s - Local maquina: %s' % (datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), ultimaDataBD, posicaoMaquina))
			
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
	global flagVerificaEmail
	
	global eventoRecebido
	global dataHoraRecebida
	global valorRecebido
	global emailRecebido
	global trnIdRecebido
	global latLongRecebida
	
	global emailEnviaMsg
	global textoEnviaMsg
	
	global idMaquina
	global posicaoMaquina
	
	print('Inicia thread de captura de email')
	
	# Reseta a flag
	flagCorpoEmail = False
	
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
		
		# Verifica se pode verificar os emails
		if(flagVerificaEmail == True):
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
				dataHoraRecebidaTempTimestamp = mail['date'].replace('0000','0300')
				tt = parsedate_tz(dataHoraRecebidaTempTimestamp)
				dataHoraRecebidaTempTimestamp = formatdate(mktime_tz(tt)).split()
				dataHoraRecebidaTimestamp = dataHoraRecebidaTempTimestamp[1] + '-' + dataHoraRecebidaTempTimestamp[2] + '-' + dataHoraRecebidaTempTimestamp[3] + ' ' + dataHoraRecebidaTempTimestamp[4]
				d = datetime.datetime.strptime(dataHoraRecebidaTimestamp, '%d-%b-%Y %H:%M:%S')
				dataHoraRecebidaTimestamp = datetime.datetime(int(d.strftime('%Y')), int(d.strftime('%m')), int(d.strftime('%d')), int(d.strftime('%H')), int(d.strftime('%M')), int(d.strftime('%S'))).timestamp()
				
				# Verifica se o email e mais novo que o ultimo evento do BD
				if(dataHoraRecebidaTimestamp > ultimaDataBDTimestamp):
					for part in mail.walk():
						# Aguarda o termino do registro no BD
						while(flagVerificaEmail == False):
							continue
							
						# Verifica se e um email de carga
						if('received a payment' in mail['subject']):
							if part.get_content_type() == 'text/plain':
								# Salva como evento de carga
								eventoRecebido = 'Carga'
								
								# Faz o parse da data/hora do email para gravacao no BD
								dataHoraRecebidaTemp = mail['date'].replace('0000','0300')
								tt = parsedate_tz(dataHoraRecebidaTemp)
								dataHoraRecebidaTemp = formatdate(mktime_tz(tt)).split()
								dataHoraRecebida = dataHoraRecebidaTemp[1] + '-' + dataHoraRecebidaTemp[2] + '-' + dataHoraRecebidaTemp[3] + ' ' + dataHoraRecebidaTemp[4]
								
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
										valorRecebido = textoRecebido[posicaoPalavra+3]
									
									# Verifica se encontrou o email que realizou o credito
									elif((textoRecebido[posicaoPalavra] == '[image:') and (textoRecebido[posicaoPalavra+1] == 'quote]') and (flagCorpoEmail == False)):
										# Seta a flag e salva o valor
										flagCorpoEmail = True
										emailRecebido = textoRecebido[posicaoPalavra+2]
									
									# Verifica se encontrou o ID da transacao do credito	
									elif((textoRecebido[posicaoPalavra] == 'Transaction') and (textoRecebido[posicaoPalavra+1] == 'ID:')):
										# Salva o ID da transacao
										trnIdRecebido = textoRecebido[posicaoPalavra+2]
					
								# Ajusta as flags
								flagVerificaEmail = False
								flagGravaBD = True

						elif('Solicita pedido' in mail['subject']):
							if part.get_content_type() == 'text/plain':
								# Salva como evento de pedido
								eventoRecebido = 'Pedido'
								
								# Faz o parse da data/hora do email para gravacao no BD
								dataHoraRecebidaTemp = mail['date'].replace('0000','0300')
								tt = parsedate_tz(dataHoraRecebidaTemp)
								dataHoraRecebidaTemp = formatdate(mktime_tz(tt)).split()
								dataHoraRecebida = dataHoraRecebidaTemp[1] + '-' + dataHoraRecebidaTemp[2] + '-' + dataHoraRecebidaTemp[3] + ' ' + dataHoraRecebidaTemp[4]
									
								# Zera a flag e carrega o payload do email
								flagCorpoEmail = False
								body = part.get_payload(decode=False)
								
								# Separa o texto do conteudo do email
								textoRecebido = body.split()
								
								# Varre todas as palavras
								for posicaoPalavra in range(len(textoRecebido)):
									# Verifica se encontrou no email a LAT/LONG
									if(('-2' in textoRecebido[posicaoPalavra]) and (flagCorpoEmail == False)):
										# Seta a flag e salva o local
										flagCorpoEmail = True
										latLongRecebida = textoRecebido[posicaoPalavra] + ' ' + textoRecebido[posicaoPalavra+1]
										
										# Carrega o status da proximidade entre os pontos
										statusDistanciaPedido = VerificaProximidade(posicaoMaquina, latLongRecebida, DISTANCIA_MAXIMA_ACEITAVEL)
									
								# Verifica se deve gravar um "DUMP" no BD
								if(statusDistanciaPedido == False):
									# Salva o valor dump
									valorRecebido = '0,00'
									
									# Salva o email origem
									emailEnviaMsg = mail['from']
									textoEmailEnviaMsg = emailEnviaMsg.split()
									for posicaoEmail in range(len(textoEmailEnviaMsg)):
										if('@' in textoEmailEnviaMsg[posicaoEmail]):
											emailEnviaMsg = textoEmailEnviaMsg[posicaoEmail]
											emailEnviaMsg = emailEnviaMsg.replace('<','')
											emailEnviaMsg = emailEnviaMsg.replace('>','')
									
									# Salva o email dump
									emailRecebido = 'DUMP'
									
									# Salva o ID da transacao
									trnIdRecebido = ''
									
								else:									
									# Salva o valor (inicialmente somente R$7,00)
									valorRecebido = '7,00'
										
									# Salva o remetente do pedido
									emailRecebido = mail['from']
									textoEmailRecebido = emailRecebido.split()
									for posicaoEmailRecebido in range(len(textoEmailRecebido)):
										if('@' in textoEmailRecebido[posicaoEmailRecebido]):
											emailRecebido = textoEmailRecebido[posicaoEmailRecebido]
											emailRecebido = emailRecebido.replace('<','')
											emailRecebido = emailRecebido.replace('>','')
									
									# Salva o email origem
									emailEnviaMsg = mail['from']
									textoEmailEnviaMsg = emailEnviaMsg.split()
									for posicaoEmail in range(len(textoEmailEnviaMsg)):
										if('@' in textoEmailEnviaMsg[posicaoEmail]):
											emailEnviaMsg = textoEmailEnviaMsg[posicaoEmail]
											emailEnviaMsg = emailEnviaMsg.replace('<','')
											emailEnviaMsg = emailEnviaMsg.replace('>','')
									
									# Salva o ID da transacao
									trnIdRecebido = ''
									
								# Ajusta as flags
								flagVerificaEmail = False
								flagGravaBD = True
								
						elif('Grava local' in mail['subject']):
							if part.get_content_type() == 'text/plain':
								# Salva como evento de pedido
								eventoRecebido = 'GravaPosicao'
								
								# Faz o parse da data/hora do email para gravacao no BD
								dataHoraRecebidaTemp = mail['date'].replace('0000','0300')
								tt = parsedate_tz(dataHoraRecebidaTemp)
								dataHoraRecebidaTemp = formatdate(mktime_tz(tt)).split()
								dataHoraRecebida = dataHoraRecebidaTemp[1] + '-' + dataHoraRecebidaTemp[2] + '-' + dataHoraRecebidaTemp[3] + ' ' + dataHoraRecebidaTemp[4]
											
								# Zera a flag e carrega o payload do email
								flagCorpoEmail = False
								body = part.get_payload(decode=False)
								
								# Separa o texto do conteudo do email
								textoRecebido = body.split()
								
								# Varre todas as palavras
								for posicaoPalavra in range(len(textoRecebido)):
									# Verifica se encontrou no email a LAT/LONG
									if(('-' in textoRecebido[posicaoPalavra]) and (flagCorpoEmail == False)):
										# Seta a flag e salva o local
										flagCorpoEmail = True
										latLongRecebida = textoRecebido[posicaoPalavra] + ' ' + textoRecebido[posicaoPalavra+1]
										
									# Verifica se encontrou o ID da maquina	
									elif(textoRecebido[posicaoPalavra] == 'Maquina'):
										# Verifica se a mensagem e para esta maquina
										if(textoRecebido[posicaoPalavra+1] == idMaquina):
											# Seta a flag de incremento de evento na fila
											flagGravaLocalMaquina = True
										
										else:
											# Reseta a flag de incremento de evento na fila
											flagGravaLocalMaquina = False
									
								# Salva o remetente da gravacao
								emailRecebido = mail['from']
								
								# Verifica se e para incrementar a fila de eventos
								if(flagGravaLocalMaquina == True):
									# Ajusta as flags
									flagVerificaEmail = False
									flagGravaBD = True
	
	# Encerra a sessao
	imapSession.close()
	imapSession.logout()

	'''except :
		print('Erro geral na tratativa de recepcao de email!')'''


#######################################################
## Funcao que trata o envio de emails
#######################################################		

def EnviaEmail():
	global flagEnviaEmail
	global flagAcionaAutomacao
	global emailEnviaMsg
	global textoEnviaMsg
	
	print('Inicia thread de envio de email')
	
	# Usuario e senha do email a conectar
	userName = 'autobeer2017'
	passwd = 'B-yxxSY3_z'

	try:
		smtpSession = smtplib.SMTP_SSL('smtp.gmail.com')
		typ, accountDetails = smtpSession.login(userName, passwd)

		# Loop
		while(True):
			# Verifica se deve enviar um email
			if(flagEnviaEmail == True):
				print('\nEnviando email...')
				
				# Reseta a flag
				flagEnviaEmail = False
				
				# Carrega os campos do email
				novaMensagem = MIMEMultipart()
				novaMensagem['Subject'] = 'Retorno AutoBeer - Maquina "' + idMaquina + '"'
				novaMensagem['From'] = 'autobeer2017@gmail.com'
				
				print(emailEnviaMsg)
				
				# Verifica se foi um pedido com sucesso para enviar ao cliente e autobeer
				if('Parabens' in textoEnviaMsg):
					address_book = ['autobeer2017@gmail.com', emailEnviaMsg]

				else:
					address_book = [emailEnviaMsg]

				novaMensagem['To'] = ','.join(address_book)
				novaMensagem.attach(MIMEText(textoEnviaMsg, 'plain'))
				
				# Envia o email
				smtpSession.sendmail('autobeer2017@gmail.com', address_book, novaMensagem.as_string())
					
		# Encerra a sessao
		smtpSession.quit()
	
	except :
		print('Erro geral na tratativa de envio de email!')
		
		
#######################################################
## Funcao que trata os pedidos
#######################################################		

def TrataPedido():
	global flagAcionaAutomacao
	global contadorPulsosSensorFluxo
	global flagPulsoSensorFluxo
	
	# Inicializa os estados
	estadoGeral = ESTADO_INICIO
	estadoLed = DESLIGA_LED
	estadoBomba = DESLIGA_BOMBA
	
	print('Inicia thread de tratamento da automacao')
	
	# Loop
	while(True):
		# Verifica se deve enviar um email
		if(flagAcionaAutomacao == True):
			print('\nLiberando automacao...')
			
			# Verifica o estado da maquina geral
			if(estadoGeral == ESTADO_INICIO):
				print('PISCA O LED')
				print('DESLIGA A BOMBA')
				print('BOTAO DESACIONADO')
				
				# Ajusta os estados
				estadoLed = PISCA_LED
				estadoBomba = DESLIGA_BOMBA
				
				# Carrega proximo estado
				estadoGeral = VERIFICA_BOTAO
				
			elif(estadoGeral == VERIFICA_BOTAO):
				# Verifica o botao
				if(estadoBotao == BOTAO_ACIONADO):
					print('BOTAO ACIONADO')
					print('ACENDE O LED')
					print('LIGA A BOMBA')
					print('VERIFICA O FLUXO')
					
					# Ajusta os estados
					estadoLed = LIGA_LED
					estadoBomba = LIGA_BOMBA
					
					# Carrega proximo estado
					estadoGeral = VERIFICA_FLUXO
					
					# Zera o contador e reseta a flag
					contadorPulsosSensorFluxo = 0
					flagPulsoSensorFluxo = False
					
				elif(estadoBotao == BOTAO_DESACIONADO):
					# Mantem o estado
					estadoGeral = VERIFICA_BOTAO
					
			elif(estadoGeral == VERIFICA_FLUXO):
				# Verifica o fluxo
				if(estadoSensorFluxo == PARA_CARGA):
					print('FLUXO: %d PULSOS' % contadorPulsosSensorFluxo)
					print('APAGA O LED')
					print('DESLIGA A BOMBA')
					
					# Ajusta os estados
					estadoLed = DESLIGA_LED
					estadoBomba = DESLIGA_BOMBA
					
					# Carrega proximo estado
					estadoGeral = ESTADO_FINAL
					
				elif(estadoSensorFluxo == LIBERA_CARGA):
					print('FLUXO: %d PULSOS' % contadorPulsosSensorFluxo)
					
					# Mantem o estado
					estadoGeral = VERIFICA_FLUXO
			
			elif(estadoGeral == ESTADO_FINAL):
				print('\nFinal do processo da automacao!!!')
				
				# Reseta a flag
				flagAcionaAutomacao = False
			
				# Seta a flag
				flagVerificaEmail = True
				
				# Carrega proximo estado
				estadoGeral = ESTADO_INICIO

			# Trata estado do led
			TrataEstadoLed(estadoLed)
			
			# Trata estado da bomba
			TrataEstadoBomba(estadoBomba)
			
			# Trata estado do botao
			estadoBotao = TrataEstadoBotao()
			
			# Trata estado do sensor de fluxo
			estadoSensorFluxo = TrataEstadoSensorFluxo()


##############################################################
## Funcao que trata os estados do led
##############################################################

def TrataEstadoLed(recebeEstadoLed):
	# Verifica o estado do led
	if(recebeEstadoLed == DESLIGA):
		# Reseta o estado do led
		gpio.output(PINO_ACENDE_LED, gpio.LOW)
		
	elif(recebeEstadoLed == LIGA):
		# Seta o estado do led
		gpio.output(PINO_ACENDE_LED, gpio.HIGH)
		
	elif(recebeEstadoLed == PISCA):
		# Seta/Reseta o estado do led  @@
		gpio.output(PINO_ACENDE_LED, gpio.LOW)


##############################################################
## Funcao que trata os estados da bomba
##############################################################

def TrataEstadoBomba(recebeEstadoBomba):
	# Verifica o estado da bomba
	if(recebeEstadoBomba == DESLIGA):
		# Reseta o estado da bomba
		gpio.output(PINO_CONTROLE_BOMBA, gpio.LOW)
		
	elif(recebeEstadoBomba == LIGA):
		# Seta o estado da bomba
		gpio.output(PINO_CONTROLE_BOMBA, gpio.HIGH)


##############################################################
## Funcao que trata os estados do botao
##############################################################

def TrataEstadoBotao():
	# Verifica se o botao esta pressionado
	if(gpio.input(PINO_BOTAO) == False):
		# Informa o estado
		enviaEstadoBotao = BOTAO_ACIONADO
		
	elif(gpio.input(PINO_BOTAO) == True):
		# Informa o estado
		enviaEstadoBotao = BOTAO_DESACIONADO
	
	return enviaEstadoBotao


##############################################################
## Funcao que trata os estados do sensor de fluxo
##############################################################

def TrataEstadoSensorFluxo():
	global contadorPulsosSensorFluxo
	global flagPulsoSensorFluxo
	
	# Verifica se o sensor recebeu pulso
	if(gpio.input(PINO_SENSOR_FLUXO) == False):
		# Seta a flag
		flagPulsoSensorFluxo = True
		
	elif((gpio.input(PINO_SENSOR_FLUXO) == True) and (flagPulsoSensorFluxo == True)):
		# Reseta a flag
		flagPulsoSensorFluxo = False
		
		# Incrementa o contador
		contadorPulsosSensorFluxo += 1
		
	# Verifica se ja encheu
	if(contadorPulsosSensorFluxo >= PULSOS_500ML):
		# Informa o estado
		enviaEstadoSensorFluxo = PARA_CARGA
		
	else:
		# Informa o estado
		enviaEstadoSensorFluxo = LIBERA_CARGA
		
	return enviaEstadoSensorFluxo


##############################################################
## Funcao que retorna o status da distancia entre dois pontos
##############################################################

def VerificaProximidade(posicao1, posicao2, distancia):
	# Verifica se nao tem a posicao
	if((posicao1 == '') or (posicao1 == '-- --')):
		# Carrega valor default
		posicao1 = '0.000000 0.0000000'
		
	# Verifica se nao tem a posicao
	if((posicao2 == '') or (posicao1 == '-- --')):
		# Carrega valor default
		posicao2 = '0.000000 0.0000000'
	
	# Ajusta o formato da lat/long
	posicao1Parse = posicao1.replace(' ', ', ')
	posicao2Parse = posicao2.replace(' ', ', ')
	
	# Carrega a distancia entre os pontos
	distanciaMedida = int(great_circle(posicao1Parse, posicao2Parse).meters)
	
	print('\nEsta a %d metros da maquina\n' % distanciaMedida)
	
	# Verifica se esta no raio configurado
	if(distanciaMedida > distancia):
		return False
		
	else:
		return True


##############################################################
## Funcao que inicializa os I/Os
##############################################################

def InicializaIOs():
	# Inicializa os GPIOs
	gpio.setmode(gpio.BOARD)
	gpio.setup(PINO_CONTROLE_BOMBA, gpio.OUT)
	gpio.setup(PINO_ACENDE_LED, gpio.OUT)
	gpio.setup(PINO_BOTAO, gpio.IN, pull_up_down = gpio.PUD_UP)
	gpio.setup(PINO_SENSOR_FLUXO, gpio.IN, pull_up_down = gpio.PUD_UP)
	
	# Reseta o estado da bomba
	gpio.output(PINO_CONTROLE_BOMBA, gpio.LOW)
	
	# Reseta o estado do led
	gpio.output(PINO_ACENDE_LED, gpio.LOW)


##########################################
## Main
##########################################

print('\n[%s] AutoBeer 2017\n' % datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

# Inicializa
InicializaIOs()

# Inicia as threads
TrataBDThread = threadTrataBD()
TrataBDThread.start()

CapturaEmailThread = threadCapturaEmail()
CapturaEmailThread.start()

EnviaEmailThread = threadEnviaEmail()
EnviaEmailThread.start()

TrataPedidoThread = threadTrataPedido()
TrataPedidoThread.start()

