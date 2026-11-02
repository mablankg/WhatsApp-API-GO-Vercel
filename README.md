# WhatsApp API Go + whatsmeow + PostgreSQL + Vercel

Primeira versão para **um número de WhatsApp por banco**. Código destinado ao repositório `mablankg/WhatsApp-API-GO-Vercel`. O aplicativo consumidor permanece em outro repositório/projeto.

## Arquitetura

- API HTTP em Go, executada como Vercel Function com preset `go`.
- Worker Go com whatsmeow em Vercel Daemon. O worker mantém a conexão com WhatsApp e busca comandos no PostgreSQL a cada segundo.
- PostgreSQL DBaaS guarda sessão, QR temporário, comandos e mensagens recebidas no schema privado `wa_private`.
- Uma trava PostgreSQL de sessão impede que dois workers utilizem o mesmo WhatsApp durante atualizações. Uma perda de conexão de coordenação encerra o worker.

**Daemons estão em beta público e executam apenas em produção.** A configuração deve ser validada na conta Vercel antes do uso real. Não há publicação ou pareamento automático por este código.

Referências: [Daemons](https://vercel.com/docs/daemons), [Go na Vercel](https://vercel.com/docs/functions/runtimes/go), [whatsmeow](https://github.com/tulir/whatsmeow).

## Configuração

1. Use o banco e usuário `alphacognus`, servidor `alphacognus.postgresql.dbaas.com.br`. Confirme no painel a porta (o exemplo usa o padrão 5432), suporte a TLS e liberação de conexões externas da Vercel. A conexão real ainda não foi testada. Use a URI PostgreSQL com `sslmode=require` e senha codificada para URL em `DATABASE_URL`. Se houver pooler, use modo sessão ou conexão direta: o lock depende da mesma sessão PostgreSQL.
2. Configure `DATABASE_URL` e `API_TOKEN` nas variáveis de ambiente **Production** da Vercel. Use token aleatório com pelo menos 32 caracteres. O exemplo em `.env.example` contém apenas um marcador para a senha; não publique credenciais no GitHub.
3. Importe este repositório na Vercel, raiz do projeto, preset Go. A região do worker está em `iad1`; escolha uma região próxima ao banco e à API, quando possível.
4. Confirme acesso a Daemons na conta e publique em produção. O worker cria o schema e as tabelas whatsmeow. A primeira conexão precisa de um usuário com permissão para criar schema/tabelas e executar as operações normais da aplicação. Se o provedor exigir certificado próprio, configure-o antes de conectar; não desative TLS para contornar um erro.
5. Verifique `/v1/status`, solicite `/v1/connect`, consulte `/v1/qr` e escaneie o PNG em **WhatsApp > Aparelhos conectados**.

Para desenvolvimento, exporte as variáveis em seu terminal (o programa não carrega `.env` automaticamente), execute `go run ./cmd/worker` em um terminal e `go run .` em outro. O worker já reconecta sessões existentes ao iniciar.

## Contrato HTTP

Todos os endpoints exigem `Authorization: Bearer SEU_TOKEN`. Respostas usam `Cache-Control: no-store`. Não há CORS: o servidor do consumidor chama a API e mantém o token fora do navegador.

| Método | Caminho | Comportamento |
|---|---|---|
| GET | `/v1/status` | Estado e heartbeat do worker |
| POST | `/v1/connect` | Enfileira conexão/geração de QR; retorna 202 e ID |
| GET | `/v1/qr` | PNG atual; 409 quando ausente/expirado |
| POST | `/v1/messages` | Enfileira texto; retorna 202 e ID |
| GET | `/v1/commands/{id}` | Resultado do comando |
| GET | `/v1/messages?after=0` | Até 100 mensagens recebidas, com cursor `next_after` |

Envio: `{"to":"5511999999999","text":"Olá!"}`. Use DDI + DDD + número, apenas dígitos.

O retorno 202 **não confirma envio**. Consulte o comando até `done`, `failed` ou `unknown`. Um comando `connect` concluído indica que a conexão foi iniciada; consulte status/QR para acompanhar o pareamento.

Em caso de interrupção durante um envio, o resultado pode ficar `unknown`. Verifique a entrega antes de reenviar. Esta versão não faz reenvio automático nem promete entrega exatamente uma vez. `message_id` indica aceitação pelo cliente WhatsApp, não leitura ou entrega ao destinatário.

O consumidor deve manter o cursor de recebimento; as mensagens são consultadas no banco, sem webhook nesta versão. Anexos recebidos registram metadados e texto vazio; download de mídia, múltiplos números, logout remoto e recibos não fazem parte desta primeira versão. Após logout pelo celular, reinicie o worker para iniciar uma nova sessão.

## Operação

- Um banco PostgreSQL dedicado por ambiente. Nunca conecte um worker de desenvolvimento ao banco de produção enquanto outro worker usa a sessão.
- Proteja as credenciais e os backups: a sessão contém material de autenticação do WhatsApp.
- As tabelas usam schema privado e RLS nas tabelas da aplicação. O serviço usa conexão PostgreSQL de servidor; o consumidor usa apenas a API.
- Dados não são apagados automaticamente. Defina retenção para mensagens/comandos antes de operar continuamente.
- Cada consulta usa limites e prazos; autenticação por token único é destinada a um consumidor servidor. Limites por cliente e múltiplos usuários exigem uma camada adicional.
- O worker e o banco consomem recursos mesmo sem mensagens; confira cotas dos seus planos.

## Verificação

`go test ./...` e `go vet ./...`. Teste real requer acesso ao PostgreSQL DBaaS e pareamento pelo titular do número. A compilação local não valida a disponibilidade do recurso Daemons na conta nem a sessão WhatsApp.
