class Cores:
    """ ANSI color codes """
    PRETO = "\033[0;30m"
    VERMELHO = "\033[0;31m"
    VERDE = "\033[0;32m"
    MARROM = "\033[0;33m"
    AZUL = "\033[0;34m"
    ROXO = "\033[0;35m"
    CIANO = "\033[0;36m"
    CINZA_CLARO = "\033[0;37m"
    CINZA_ESCURO = "\033[1;30m"
    VERMELHO_CLARO = "\033[1;31m"
    VERDE_CLARO = "\033[1;32m"
    AMARELO = "\033[1;33m"
    AZUL_CLARO = "\033[1;34m"
    ROXO_CLARO = "\033[1;35m"
    CIANO_CLARO = "\033[1;36m"
    BRANCO_CLARO = "\033[1;37m"
    NEGRITO = "\033[1m"
    FAINT = "\033[2m"
    ITALICO = "\033[3m"
    SUBLINHADO = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVO = "\033[7m"
    TRACADO = "\033[9m"
    RESET = "\033[0m"
    

    if not __import__("sys").stdout.isatty():
        for _ in dir():
            if isinstance(_, str) and _[0] != "_":
                locals()[_] = ""
    else:

        if __import__("platform").system() == "Windows":
            kernel32 = __import__("ctypes").windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            del kernel32