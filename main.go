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
	"strconv"
	"strings"
)

type TrManagerItem struct {
	trID int
	status int
}

type LockTableItem struct {
	idItem string
	trID   int
	escopo int
	duracao int
	tipo int
}

type WaitForItem struct {
	idItem    string
	operacoes []*LockTableItem
}

type Tupla struct {
	p1 int
	p2 int
}

func BT(trManager *[]*TrManagerItem, trID int) {

	transacao := TrManagerItem{
		trID:   trID,
		status: 0,
	}

	*trManager = append(*trManager, &transacao)
}

func RL(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID && bloqueio.tipo == 1 {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("A Transação %d obteve bloqueio de Leitura sobre o item %s", operacao.trID, operacao.idItem))

			*lockTable = append(*lockTable, operacao)

			if(operacao.duracao == 0){
				UL(trManager, lockTable, waitFor, grafoEspera, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}

	return -1

}

func WL(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, operacao *LockTableItem) int {

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID && transacao.status == 0 {
			for _, bloqueio := range *lockTable {
				if bloqueio.idItem == operacao.idItem && bloqueio.trID != operacao.trID {
					return bloqueio.trID
				}

			}

			fmt.Println(fmt.Sprintf("A Transação %d obteve bloqueio de Escrita sobre o item %s", operacao.trID, operacao.idItem))

			*lockTable = append(*lockTable, operacao)

			if(operacao.duracao == 0){
				UL(trManager, lockTable, waitFor, grafoEspera, operacao.trID, operacao.idItem)
			}

			return -1

		}
	}

	return -1

}

func UL(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, trID int, idItem string) {

	for idx_bloqueio, bloqueio := range *lockTable {
		if idItem != "" {
			if bloqueio.idItem == idItem && bloqueio.trID == trID {

				*lockTable = append((*lockTable)[:idx_bloqueio], (*lockTable)[idx_bloqueio+1:]...)

				escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)

				var tipo_bloqueio string
				if bloqueio.tipo == 1 {
					tipo_bloqueio = "Escrita"
				} else {
					tipo_bloqueio = "Leitura"
				}

				fmt.Println(fmt.Sprintf("A Transação %d liberou o bloqueio de %s sobre o item %s", trID, tipo_bloqueio, idItem))
			}

		} else {
			if bloqueio.trID == trID {
				if len(*lockTable) < 2 {
					*lockTable = (*lockTable)[:0]

				} else if idx_bloqueio+1 > len(*lockTable)-1 {
					*lockTable = (*lockTable)[:len(*lockTable)-1]
				} else {
					*lockTable = append((*lockTable)[:idx_bloqueio], (*lockTable)[idx_bloqueio+1:]...)
				}

				escalonarWaitFor(trManager, lockTable, waitFor, grafoEspera, bloqueio.idItem)

			}
		}

	}
}

func C(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, trID int) {

	for _, transacao := range *trManager {
		if transacao.trID == trID {
			transacao.status = 1
		}
	}

	UL(trManager, lockTable, waitFor, grafoEspera, trID, "")
}

func WAIT(trManager *[]*TrManagerItem, grafoEspera *[]Tupla, waitFor *[]*WaitForItem, operacao *LockTableItem, transacao_detentora int) Tupla {

	tupla_padrao := Tupla{-1, -1}

	if operacao.trID > transacao_detentora {

		for _, transacao := range *trManager {

			if transacao.trID == operacao.trID {
				transacao.status = 2
			}
		}

		fmt.Println(fmt.Sprintf("A Transação %d foi abortada dada a estratégia Wait Die ", operacao.trID, transacao_detentora, operacao.idItem))
		return tupla_padrao
	}

	for _, tupla := range *grafoEspera {
		if tupla.p1 == operacao.trID && tupla.p2 == transacao_detentora {

			fmt.Println(fmt.Sprintf("A Transação %d gerou um Deadlock com a Transação %d", tupla.p1, tupla.p2))
			return tupla
		}
	}

	nova_tupla := Tupla{transacao_detentora, operacao.trID}

	*grafoEspera = append(*grafoEspera, nova_tupla)

	fmt.Println(fmt.Sprintf("A operação %d entrou na Fila de Espera pela liberação do Item %s pela Transação %d", operacao.trID, operacao.idItem, transacao_detentora))

	for _, transacao := range *trManager {
		if transacao.trID == operacao.trID {
			transacao.status = 3
		}
	}

	for _, wf_item := range *waitFor {
		if wf_item.idItem == operacao.idItem {
			wf_item.operacoes = append(wf_item.operacoes, operacao)
			return tupla_padrao
		}
	}

	var lt []*LockTableItem
	lt = append(lt, operacao)

	wf_item := WaitForItem{
		idItem:    operacao.idItem,
		operacoes: lt,
	}

	*waitFor = append(*waitFor, &wf_item)

	return tupla_padrao

}

func escalonarWaitFor(trManager *[]*TrManagerItem, lockTable *[]*LockTableItem, waitFor *[]*WaitForItem, grafoEspera *[]Tupla, idItem string) {

	for _, wf_item := range *waitFor {

		if wf_item.idItem == idItem {
			if len(wf_item.operacoes) < 1 {
				return
			}
			operacao := wf_item.operacoes[0]
			wf_item.operacoes = wf_item.operacoes[1:]

			for _, transacao := range *trManager {
				if transacao.trID == operacao.trID {
					transacao.status = 0
				}
			}

			if operacao.tipo == 1 {
				// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
				res_WL := WL(trManager, lockTable, waitFor, grafoEspera, operacao)

				if res_WL != -1 {
					WAIT(trManager, grafoEspera, waitFor, operacao, res_WL)
				}

			} else {
				// fmt.Println(fmt.Sprintf("Transação %d - Solicita bloqueio de Escrita sobre o item %s", trID, idItem))
				res_RL := RL(trManager, lockTable, waitFor, grafoEspera, operacao)

				if res_RL != -1 {
					WAIT(trManager, grafoEspera, waitFor, operacao, res_RL)
				}
			}
		}
	}
}

func devolverTextoColorido(text string, color string) string {
	novaString := color + text + color;
	return novaString;
}
// Status: 0-> ativa; 1-> concluída; 2-> abortada; 3-> esperando.
func statusParaString(valorNumericoDoStatus int) string {
	switch valorNumericoDoStatus {
	case 0:
		return "ativa";
	case 1:
		return "concluída";
	case 2:
		return "abortada";
	case 3:
		return "esperando";
	default:
		return " ";
	}
}


func main() {

	var trManager []*TrManagerItem
	var lockTable []*LockTableItem
	var waitFor []*WaitForItem
	var grafoEspera []Tupla
	var nivel_isolamento int
	var d_leitura int
	var d_escrita int

	//var opcao_isolamento int;
	var str string;

	fmt.Println("Formato da transação: BT(1)r1(x)BT(2)w2(x)r2(y)r1(y)C(1)r2(z)C(2)");
	fmt.Print("Digite a transação a ser executada: ");
	fmt.Scanln(&str);
	fmt.Println("Escolha um nível de isolamento: 0 - Rean Uncommitted // 1 - Read Committed // 2 - Repeatable Read // 3 - Serializable");
	fmt.Print("Digite o nível de isolamento: ");
	fmt.Scanln(&nivel_isolamento);
	fmt.Println("Registros Log");

	str = strings.ToUpper(str)

	partes := strings.Split(str, ")")
	partes = partes[:(len(partes) - 1)]

	if(nivel_isolamento == 0){
		d_escrita = 0
		d_leitura = 0

	}else if (nivel_isolamento == 1){
		d_escrita = 1
		d_leitura = 0

	}else if (nivel_isolamento == 2){
		d_escrita = 1
		d_leitura = 1
	}else{
		d_escrita = 1
		d_leitura = 1
	}

	for _, operacao := range partes {

		if string(operacao[0]) == "B" {
			trID, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			fmt.Println("A Transação %d começou", trID)
			BT(&trManager, trID)

			fmt.Println()

		} else if string(operacao[0]) == "W" {
			trID, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {
					operacao := LockTableItem{
						idItem:  idItem,
						trID:    trID,
						escopo:  0,
						duracao: d_escrita,
						tipo:    1,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("A Transação %d solicitou bloqueio de escrita sobre o item %s", "\033[33m"), trID, idItem))
					res_WL := WL(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_WL != -1 {
						WAIT(&trManager, &grafoEspera, &waitFor, &operacao, res_WL)
					}

					fmt.Println()
				}
			}

		} else if string(operacao[0]) == "R" {
			trID, _ := strconv.Atoi(string(operacao[1]))
			idItem := string(operacao[len(operacao)-1])

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {

					operacao := LockTableItem{
						idItem:  idItem,
						trID:    trID,
						escopo:  0,
						duracao: d_leitura,
						tipo:    0,
					}

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita bloqueio de Leitura sobre o item %s", "\033[33m"), trID, idItem))
					res_RL := RL(&trManager, &lockTable, &waitFor, &grafoEspera, &operacao)

					if res_RL != -1 {
						WAIT(&trManager, &grafoEspera, &waitFor, &operacao, res_RL)
					}

					fmt.Println()
				}
			}

		} else if string(operacao[0]) == "C" {
			trID, _ := strconv.Atoi(string(operacao[len(operacao)-1]))

			for _, transacao := range trManager {

				if transacao.trID == trID && transacao.status != 2 {

					fmt.Println(fmt.Sprintf(devolverTextoColorido("|| === Transação %d - Solicita Commit", "\033[33m"), trID))
					C(&trManager, &lockTable, &waitFor, &grafoEspera, trID)

					fmt.Println()
				}
			}

		}

		fmt.Println("|| === PRINTANDO TABELA TR MANAGER");
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		fmt.Println(devolverTextoColorido("||       ID            STATUS     ", "\033[31m"));
		for _, item := range trManager {
			linha := "||       " + strconv.Itoa((*item).trID) + "             " + statusParaString((*item).status) + "    ";
			fmt.Println(devolverTextoColorido(linha, "\033[31m"));
		}
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		
		fmt.Println("|| === PRINTANDO TABELA LOCK TABLE");
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		fmt.Println(devolverTextoColorido("|| ITEM   ID    ESCO   DURA  TIP0 ", "\033[31m"));
		for _, item := range lockTable {
			linha := "|| " + (*item).idItem + "      " + strconv.Itoa((*item).trID) + "      " + strconv.Itoa((*item).escopo)+"     "+strconv.Itoa((*item).duracao)+"     "+strconv.Itoa((*item).tipo);
			fmt.Println(devolverTextoColorido(linha, "\033[31m"));
		}
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		
		fmt.Println("|| === PRINTANDO TABELA WAIT FOR TABLE");
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		fmt.Println(devolverTextoColorido("|| ID    OPERACAO                 ", "\033[31m"));
		/* indice := 0; */
		if (len(waitFor) >= 1) {
			linha := "|| " + (*waitFor[0]).idItem + "     " + (*waitFor[0]).idItem + "      " + strconv.Itoa((*waitFor[0]).operacoes[0].trID) + "      " + strconv.Itoa((*waitFor[0]).operacoes[0].escopo)+"     "+strconv.Itoa((*waitFor[0]).operacoes[0].duracao)+"     "+strconv.Itoa((*waitFor[0]).operacoes[0].tipo)
			fmt.Println(devolverTextoColorido(linha, "\033[31m"));
			for index, item := range waitFor[1:] {
				linha = "||       " + (*item).idItem + "      " + strconv.Itoa((*item).operacoes[index].trID) + "      " + strconv.Itoa((*item).operacoes[index].escopo)+"     "+strconv.Itoa((*item).operacoes[index].duracao)+"     "+strconv.Itoa((*item).operacoes[index].tipo)
				fmt.Println(devolverTextoColorido(linha, "\033[31m"));
			}
		}
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));

		fmt.Println("|| === PRINTANDO TABELA GRAFO DE ESPERA");
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
		fmt.Println(devolverTextoColorido("||       P1            P2     ", "\033[31m"));
		for _, item := range grafoEspera {
			linha := "||       " + strconv.Itoa(item.p1) + "             " + strconv.Itoa(item.p2) + "    ";
			fmt.Println(devolverTextoColorido(linha, "\033[31m"));
		}
		fmt.Println(devolverTextoColorido("|| ===============================", "\033[31m"));
	}

	

}
