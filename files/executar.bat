@echo off
REM Muda para o diretório correto
cd /d C:\Users\ichib\PycharmProjects\Pyro\files

REM Inicia o serviço de nomes
echo Iniciando o serviço de nomes...
start cmd /k "python -m Pyro5.nameserver"

REM Aguarda um pouco para o serviço de nomes iniciar
timeout /t 5 >nul

REM Inicia o líder
echo Iniciando o líder...
start cmd /k "python lider.py"

REM Aguarda um pouco para o líder inicializar
timeout /t 2 >nul

REM Inicia os seguidores
echo Iniciando seguidores...
start cmd /k "python seguidor.py votante Votante1"
start cmd /k "python seguidor.py votante Votante2"
start cmd /k "python seguidor.py observador Observador1"

REM Aguarda um pouco para os seguidores inicializarem
timeout /t 2 >nul

REM Inicia o publicador
echo Iniciando o publicador...
start cmd /k "python publicador.py"

REM Inicia o consumidor
echo Iniciando o consumidor...
start cmd /k "python consumidor.py"

echo Todos os componentes foram iniciados. Pressione qualquer tecla para fechar este terminal...
pause
