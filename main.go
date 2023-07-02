// gerenciador de transações de banco de dados
// com o suporte para o controle de concorrência baseado na técnica de bloqueio em duas fases restrito

// Classe TrManager

//A classe TrManager gerencia o estado de cada uma das transações concorrentes. Essa
//classe deve ter dois atributos. O atributo TrId, do tipo inteiro, será o identificador da
//transação e representará um “timestamp”. Assim, a transação de TrId=2 é mais velha que a
//transação de TrId=1. O atributo Status irá armazenar o estado da transação: ativa,
//concluída, abortada, esperando.

// Classe LockTable

//A classe LockTable gerencia a tabela de bloqueios. Essa classe deve ter cinco atributos. O
//atributo IdItem representa o identificador do item bloqueado. O atributo TrId representa o
//identificador da transação que bloqueou o item de identificador IdItem. O atributo Escopo
//representa o escopo do bloqueio: objeto ou predicado. O atributo Duração representa a
//duração do bloqueio: curta ou longa. O atributo Tipo representa o tipo do bloqueio: leitura
//ou escrita.

//A classe LockTable deve ter pelo menos as seguintes funções:
//• RL(Tr, D) : Insere um bloqueio de leitura na LockTable sobre o item D para a
//transação Tr, se for possível.
//• WL(Tr, D) : Insere um bloqueio de escrita na LockTable sobre o item D para a
//transação Tr, se for possível.
//• UL(Tr, D) : Apaga o bloqueio da transação Tr sobre o item D na LockTable.


//Implemente um Grafo de Espera (Wait For) para identificar deadlocks.

//Implemente uma estrutura de dados chamada WaitItem para manter, para cada item de dado
//bloqueado “i”, uma lista FIFO com os identificadores de transações que estão esperando
//pelo item “i”.

//Implemente uma classe chamada Scheduler para realizar o controle de concorrência. Essa
//classe deve implementar a técnica de bloqueio em duas fases restrito. Deve ainda realizar o
//controle de deadlocks baseado na estratégia de Wait Die.

//A interface de entrada do programa principal dever a permitir ao usuário:

//i. Definir o nível de isolamento a ser utilizado (read uncommitted, read committed,
//repeatable read ou serializable)

//ii. Entrar com escalonamento do tipo BT(1)r1(x)BT(2)w2(x)r2(y)r1(y)C(1)r2(z)C(2)
//onde:

//• BT(X): inicia a transação X
//• r1(x): transação 1 deseja ler o item x, portanto solicita bloqueio de leitura sobre o
//item x
//• w1(x): transação 1 deseja escrever o item x, portanto solicita bloqueio de escrita
//sobre o item x
//• C(X): Validação da transação X, quando todos os seus bloqueios (de longa duração)
//devem ser liberados

//iii. Seguir o escalonamento recebido, exibindo as operações realizadas, bem como o
//estado das estruturas de dados utilizadas, à cada operação.

package main

import (
	"fmt"
	"sync"
)

type TrManager struct {
	TrId   int
	Status int // 0 = ativa, 1 = concluida, 2 = abortada, 3 = esperando
}

type LockTable struct {
	IdItem  int
	TrId    int	
	Escopo  string // 0 = objeto, 1 = predicado
	Duracao string // 0 = curta, 1 = longa
	Tipo    string // 0 = leitura, 1 = escrita
}

type Waitfor struct {
	IdItem int
	operacoes []*LockTableItem
}

type Tupla struct {
	p1 int
	p2 int
}

func BT(TrManager *[]*TrManager, TrID int) {

	transacao := TrManager{
		TrID:   TrID,
		Status: 0,
	}

	*trManager = append(*trManager, &transacao)
}



func main() {
	trManager := TrManager{
		TrId:   1,
		Status: "ativa",
	}

	lockTable := LockTable{}

	// Chama a função RL para inserir um bloqueio de leitura na LockTable
	successRL := lockTable.RL(trManager.TrId, 1)

	if successRL {
		fmt.Println("Bloqueio de leitura inserido na LockTable:", lockTable)
	} else {
		fmt.Println("Não foi possível inserir o bloqueio de leitura na LockTable.")
	}

	// Chama a função UL para remover o bloqueio da LockTable
	lockTable.UL(trManager.TrId, 1)

	fmt.Println("Bloqueio removido da LockTable:", lockTable)
}


func (lt *LockTable) RL(Tr int, D int) bool {
	// Verifica se o item já está bloqueado por uma transação diferente
	if lt.IdItem == D && lt.TrId != Tr {
		return false
	}

	// Insere o bloqueio de leitura na LockTable
	lt.IdItem = D
	lt.TrId = Tr
	lt.Escopo = "objeto"
	lt.Duracao = "curta"
	lt.Tipo = "leitura"

	return true
}

func (lt *LockTable) WL(Tr int, D int) bool {
	// Verifica se o item está bloqueado por outra transação
	if lt.IdItem == D && lt.TrId != Tr {
		return false
	}

	// Insere o bloqueio de escrita na LockTable
	lt.IdItem = D
	lt.TrId = Tr
	lt.Escopo = "objeto"
	lt.Duracao = "curta"
	lt.Tipo = "escrita"

	return true
}

func (lt *LockTable) UL(Tr int, D int) {
	// Verifica se o item está bloqueado pela transação especificada
	if lt.IdItem == D && lt.TrId == Tr {
		// Remove o bloqueio da LockTable
		lt.IdItem = 0
		lt.TrId = 0
		lt.Escopo = ""
		lt.Duracao = ""
		lt.Tipo = ""
	}
}