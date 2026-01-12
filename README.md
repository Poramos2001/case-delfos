# case-delfos
precisa ter um super user e botar o login e senha dele no config.json

# TODO
- mudar a logica de sys.exit pra ou passar se n tem problema ou efetivamente raise o erro, lembrando q o main anda tem q poder continuar ser usado como script, ent muda o main pra dar os sys.exit(1) se vier algum erro (wrap com try except) e if df.empy sys.exit(0)
- consertar o logger
- fazer o job e schedule
- containerizo com docker