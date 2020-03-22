---
date: 2020-02-14 13:32:04
layout: post
title: "Usando Spring Batch para fazer ETL de informaçoes financeiras (Parte I)"
subtitle: Lorem ipsum dolor sit amet, consectetur adipisicing elit.
description: Lorem ipsum dolor sit amet, consectetur adipisicing elit.
image: https://res.cloudinary.com/dm7h7e8xj/image/upload/v1559824575/theme14_gi2ypv.jpg
optimized_image: https://res.cloudinary.com/dm7h7e8xj/image/upload/c_scale,w_380/v1559824575/theme14_gi2ypv.jpg
category: Dev
tags:
  - Java
  - Spring Framework
  - Spring Batch
  - ETL
author: thiagorossener
paginate: false
---

Comecei a brincar com Spring Batch e ETLs e isso foi o que consegui fazer (até então)….

O ano de 2019 foi bem intenso pra mim pois tive a oportunidade de aprender muita coisa sobre desenvolvimento Web com Java Spring e cultura DevOps no geral. Mas aí é o que dizem: Quanto mais você estuda algo mais você percebe o quão ainda tem que aprender. Então lá estava eu procurando uma forma de aprofundar meus conhecimentos do ecossistema Spring, quando fiquei curioso com o projeto Spring Batch.

Pra quem não sabe Spring Batch é um subprojeto do framework Spring destinado a fazer processamentos em lote. Este projeto possui diversas facilidades incluídas na sua toolset que permite a execução sistemática, determinística e rastreável de trabalhos executados em batches ou lotes. Um exemplo muito bacana de aplicação, é o seu uso para processar um streaming de dados tais como uma planilha ou um banco de dados. E é justamente um exemplo assim que eu vou mostrar aqui inaugurando o meu primeiro artigo for real do meu blog =D

Na minha busca por um caso de uso bacana para entender um pouco mais o Spring Batch, e aproveitando que ano passado estava inserido no Grupo de Estudos em Ciência de Dados, um projeto de extensão da Unila que você pode conferir mais clicando neste [link](https://medialabfoz.com/gecd/), me deparei com o conceito de ETL.

## ELT (TL;DR)

Um ETL é uma sigla inglesa que significa Extract Transform e Load (Extrair, Transformar e “Carregar”). A idéia em si é muito simples e consiste em operações que envolvem grande volumes de dados compostas em basicamente em três fases: extração, transformação e carregamento (ou armazenamento).

Seu uso é muito comum quando se quer transferir um ou até mesmo vários dados de um conjunto de armazenamento para o outro. Sendo que entre origem e destino é possível fazer transformações nos dados tais como agregação, formatação e entre outros.

## Descrição do problema que eu fui caçar

Com este conceito em mente, precisava de um conjunto de dados para começar. Como eu me amarro em aprender mais sobre produtos financeiros, tais como renda fixa, ações e fundos imobiliários fui procurar alguma coisa que envolvesse esses tipos de dados. Descobri que a CVM (Comissão de Valores Imobiliários) possui um portal de dados abertos(fazer link) onde é possível baixar as mais diversas planilhas relacionadas a fundos de investimento tais como informe diário, informações cadastrais e entre outros.

Dentre os conjuntos de dados fornecidos pela CVM existe um denominado “Informe diário” que segundo a própria CVM tem um demonstrativo com informações como:

- Valor total da carteira do fundo
- Patrimônio líquido
- Valor da cota
- Captações realizadas no dia
- Resgates pagos no dia
- Número de cotistas

Baixei uma planilha de um dia aleatório e boom! Um csv com mais de 200.000 linhas de dados estruturados. Perfeito para arregaçar as mangas e começar a brincar.

Cada linha do CSV representa informações de um fundo em determinado dia. Dentre as dezenas de colunas disponibilizadas na planilha, elegi algumas para utilizar na minha solução de ETL, são elas:

1. CNPJ da empresa emissora
2. Data de Referência
3. Valor Total
4. Valor da Quota
5. Patrimônio líquido
6. Captação no dia (total de depósitos)
7. Resgates no dia (total de retiradas)
8. Número de Quotistas

Como primeiro passo pensei em só fazer um ETL simples, ou seja carregar as informações do CSV e gravá-las em um banco de dados MySQL. Para não ficar sem fazer nenhuma mudança no passo de transformação, resolvi apenas remover a formatação do campo CNPJ, ou seja, no banco eu somente salvo os dígitos, sem pontos ou traços.

O rascunho de guardanapo da minha solução ficou assim:

A ideia é, ler o arquivo CSV, fazer um ETL usando Spring Batch, gravar as informações em uma tabela no banco de dados usando o Spring Data, e finamente utilizar o Spring Web para disponibilizar as informações em formato JSON através de uma API.

O esquema de dados da entidade que eu criei no banco ficou assim:

[![DailyInform](https://i.imgur.com/Wp41TkJ.png "Daily Inform")](https://i.imgur.com/Wp41TkJ.png){:data-lightbox="DailyInform"}{:data-title="Entidade Daily Inform"}

Apresentados a ideia e uma arquitetura geral do brincadeira, bora escrever um pouco de código!

## Passo 1, fazendo meu “Hello World” com Spring Batch

Para utilizar o Spring Batch primeiro você precisa colocar as dependências dele no seu projeto. Você pode começar criando um projeto no Spring Initializr selecionando as seguintes dependências:

- Spring boot starter data jpa: (para fazer conexão ao banco de dados)
- Spring boot starter web: (para criar a camada web responsável por expor os dados vai uma API REST com JSON).
- mySql-connector-java: Para este tutorial eu optei por utilizar o MySQL, mas qualquer outro DB relacional funciona.
- spring-boot-starter-batch: A estrelinha do projeto, contém as classes necessárias para utilizar o Spring Batch.

[![SpringInitializr](https://i.imgur.com/SwwyBT1.png "Spring Initializr")](https://i.imgur.com/SwwyBT1.png){:data-lightbox="SpringInitializr"}{:data-title="Montando as dependênias do projeto como Spring Initializr"}

Uma vez baixado o zip é só descompactar, abrir na sua IDE favorita e começar a codar!

Primeiramente é necessário criar a Entidade DailyInform, que será o objeto final que as informações de planilha serão convertidas e gravadas no Banco:

```java
import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(indexes = {
       @Index(columnList = "cnpj", name = "cnpj_hidx"),
       @Index(columnList = "referenceDate", name = "reference_date_hidx")},
       uniqueConstraints = @UniqueConstraint(columnNames = { "cnpj", "referenceDate" }))
public class DailyInform implements Serializable {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO)
   private Long id;

   @NotNull
   @CNPJ
   @Column(nullable = false)
   private String cnpj;

   @NotNull
   @Column(nullable = false)
   private LocalDate referenceDate;

   @NotNull
   @Column(nullable = false)
   private BigDecimal totalValue;

   @NotNull
   @Column(nullable = false)
   private BigDecimal quotaValue;

   @NotNull
   @Column(nullable = false)
   private BigDecimal netWorth;

   @NotNull
   @Column(nullable = false)
   private BigDecimal totalDeposits;

   @NotNull
   @Column(nullable = false)
   private BigDecimal totalWithdrawals;

   @NotNull
   @Column(nullable = false)
   private Long numberOfQuotaHolders;

   public DailyInform() {};

   public DailyInform(String cnpj, LocalDate referenceDate,
                      BigDecimal totalValue, BigDecimal quotaValue,
                      BigDecimal netWorth, BigDecimal totalDeposits,
                      BigDecimal totalWithdrawals, Long numberOfQuotaHolders) {
       this.cnpj = cnpj;
       this.referenceDate = referenceDate;
       this.totalValue = totalValue;
       this.quotaValue = quotaValue;
       this.netWorth = netWorth;
       this.totalDeposits = totalDeposits;
       this.totalWithdrawals = totalWithdrawals;
       this.numberOfQuotaHolders = numberOfQuotaHolders;
   }

   @Override
   public String toString() {
       return "DailyInform{" +
               "cnpj='" + cnpj + '\'' +
               ", referenceDate=" + referenceDate +
               ", quotaValue=" + quotaValue +
               '}';
   }
   // .... Getters/Setters
}
```

Observe que é uma Entidade JPA bem simples só para ilustrar os conceitos. Também adicionei alguns índices para melhorar o desempenho de queries de busca.

Os processamentos em lote no Spring são executados através de um Job. Um job é composto de Steps que definem passos sucessivos de leitura, transformação e escrita (para este tutorial faremos um Job com um único Step). O Step basicamente tem três elementos:

1. Um reader: Define o que e como vai ser lido. Nesta fase que se define de onde virão os dados (no caso a planilha), e quais colunas serão digeridas.
2. Um processor: Define uma operação de transformação que o dado lido terá que sofrer até estar pronto para a escrita. Recebe um objeto de entrada e retorna outro objeto de saída.
3. Um writer: Define onde os dados recém processados serão escritos, neste artigo iremos utilizar um Repository bean para escrever as informações na tabela do banco de dados.

Para utilizar um Job no SpringBatch é necessário configurá-lo. Segue um exemplo de um trecho do arquivo @Configuration(lembrando que o código completo deste tutorial encontra-se no meu GitHub:

```java
@Configuration
@EnableBatchProcessing
public class SpringBatchConfiguration {

   @Bean
   public Job job(JobBuilderFactory jobBuilderFactory,
                  StepBuilderFactory stepBuilderFactory,
                  ItemReader<DailyInform> itemReader,
                  ItemProcessor<DailyInform, DailyInform> itemProcessor,
                  ItemWriter<DailyInform> itemWriter) {

       Step step = stepBuilderFactory.get("ETL-file-load")
               .<DailyInform, DailyInform>chunk(1000)
               .reader(itemReader)
              .processor(itemProcessor)
               .writer(itemWriter)
               .build();

       return jobBuilderFactory.get("ETL-Load")
                               .incrementer(new RunIdIncrementer())
                               .start(step)
                               .build();
   }
   // mais beans virão
}

```

Como pode ser visto na linha 25, é necessário utilizar a annotation `@EnableBatchProcessing` para habilitar o Spring Batch no projeto. O Job é exposto para aplicação através de um bean, sendo gerenciado pelo Spring Container. O método job recebe cinco parâmetros: `jobBuilderFactory`, `stepBuilderFactory`, `itemReader`, `itemProcessor`, `itemWriter`. As factories são injetadas pelo próprio Spring, ao passo que cabe a nós definir os outros parâmetros através de Beans na aplicação. Uma vez definidos, o próprio Spring os injeta no método e cria o `Job`. Observe que os tipos do reader, processor e writer são tipados de acordo com a entidade modelada `DailyInform`. É perfeitamente possível modelar um fluxo onde a leitura é de um tipo, convertido a outro no processamento e depois gravado na estágio de leitura. Mas para manter este tutorial mais simples vamos seguir com a idéia de manter o mesmo tipo durante todo o ciclo.

Graças a injeção de dependência, o Spring consegue montar o Job utilizando um reader, processor e writer que também são Beans na aplicação. Vou explicar como eles são criados e o detalhamento de cada um deles nas sessões a seguir.

## Passo 2 - Criando o bean de leitura

Podemos criar um bean de leitura a partir da classe `FlatFileItemReader<T>`, uma classe que implementa indiretamente a interface `ItemReader<T>`. A facilidade de usar esta classe é que ela foi projetada para cenários de leituras em arquivos linha a linha (perfeito para o nosso exemplo de CSV). A sua configuração é relativamente simples, no entanto ela exige a criação de outro bean auxiliar. Segue um exemplo da implementação:

```java
@Bean
public FlatFileItemReader<DailyInform> fileItemReader(@Value("${input}") Resource resource) {
   FlatFileItemReader<DailyInform> fileItemReader = new FlatFileItemReader<>();
   fileItemReader.setResource(resource);
   fileItemReader.setEncoding("ISO-8859-3");
   fileItemReader.setName("CSV-Reader");
   fileItemReader.setLinesToSkip(1);
   fileItemReader.setLineMapper(lineMapper());
   return fileItemReader;
}
```

Em que:

Entrada: Resource (uma referência ao arquivo .csv), aqui eu setei o path em uma variável de ambiente `${input}` por conveniência.

Saída: Um objeto do tipo `FlatFileItemReader<DailyInform>` onde eu configuro:

- O recurso que será lido.
- O encoding do arquivo, no caso eu descobri que a planilha baixada não está em UTF-8, logo eu precisei informar o encoding correto para não haver falhas de leitura de caracteres especiais tais como à ó ã (Português né ?).
- Nome deste bean: provavelmente utilizado pelo Spring para uma indexação interna deste reader.
- Linhas para pular: Coloquei o valor 1 para pular o cabeçalho e ir direto para os dados.
- O `LineMapper`: O bean auxiliar necessário para configurar as políticas de leitura. Sua criação será detalhada a seguir.

### Criando o LineMapper

O `LineMapper` é o bean que basicamente define qual vai ser a lógica de leitura tais como: Qual caractere é o delimitador de colunas, qual é o nome das colunas e como converter a linha lida em objeto. Sua construção se dá da seguinte forma:

```java
@Bean
public LineMapper<DailyInform> lineMapper() {
   DefaultLineMapper<DailyInform> defaultLineMapper = new DefaultLineMapper<>();
   DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

   lineTokenizer.setDelimiter(";");

   lineTokenizer.setNames("CNPJ_FUNDO",
                          "DT_COMPTC",
                          "VL_TOTAL",
                          "VL_QUOTA",
                          "VL_PATRIM_LIQ",
                          "CAPTC_DIA",
                          "RESG_DIA",
                          "NR_COTST");
   lineTokenizer.setStrict(false);

   DailyInformFieldSetMapper dailyInformFieldSetMapper = new DailyInformFieldSetMapper();

   defaultLineMapper.setLineTokenizer(lineTokenizer);
   defaultLineMapper.setFieldSetMapper(dailyInformFieldSetMapper);

   return defaultLineMapper;
}

```

O objeto retornado por este método é uma instância da classe `DefaultLineMapper`, que implementa a interface `LineMapper`, onde é configurado:

- O delimitador: no caso do CSV da CVM foi utilizado o caractere ‘;’ para separar os dados de cada coluna
- Define os tokens (ou os nomes) de cada coluna que será lida, bem como a quantas serão processadas. Mais tarde estes nomes serão utilizados no bean que coloca as informações de cada linha em um objeto POJO DailyInform.
- Setar leitura estrita: Caso seja true toda linha lida deve ter o número correto de colunas, se for marcada como false o SpringBatch aceita a leitura de linhas que não possui o mesmo número de colunas definidos nos tokens. Caso haja menos colunas, os valores que faltam serão preenchidos com empty(vazio), caso haja mais colunas elas serão simplesmente ignoradas.
- O `fieldsetmapper`: Bean que define como os dados lidos das linhas serão convertidos em um POJO. Neste caso eu criei uma classe chamada DailyInformFieldSetMapper que faz este trabalho.

### O DailyInformFieldSetMapper (pois aqui não existe mágica)

Esta classe implementa a interface `FieldSetMapper<T>`, que por contrato obriga a implementar o método `<T> mapFieldSet(FieldSet fieldSet)`, este método recebe por parâmetro uma linha lida pelo `LineMapper` e é nele que se implementa a lógica de colocar os tokens definidos em atributos da classe `DailyInform`. Esse é o coração da conversão do dado que está em uma linha CSV para um objeto Java completo (por isso que eu digo que aqui não existe mágica xD). A classe só tem o méotodo `mapFieldSet` e fica desta forma:

```java
public class DailyInformFieldSetMapper implements FieldSetMapper<DailyInform> {

   @Override
   public DailyInform mapFieldSet(FieldSet fieldSet) throws BindException {
       final DailyInform dailyInform = new DailyInform();
       dailyInform.setCnpj(fieldSet.readString("CNPJ_FUNDO"));
       dailyInform.setReferenceDate(fieldSet.readDate("DT_COMPTC")
                  .toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
       dailyInform.setTotalValue(fieldSet.readBigDecimal("VL_TOTAL"));
       dailyInform.setQuotaValue(fieldSet.readBigDecimal("VL_QUOTA"));
       dailyInform.setNetWorth(fieldSet.readBigDecimal("VL_PATRIM_LIQ"));
       dailyInform.setTotalDeposits(fieldSet.readBigDecimal("CAPTC_DIA"));
       dailyInform.setTotalWithdrawals(fieldSet.readBigDecimal("RESG_DIA"));
       dailyInform.setNumberOfQuotaHolders(fieldSet.readLong("NR_COTST"));
       return dailyInform;
   }
}

```

Feito isso, tudo está pronto e configurado para o passo de leitura, agora só falta os passos de transformação e escrita, mas felizmente eles são bem mais simples de fazer conforme eu vou mostrar agora.

## Parte 3 - Configurando o Processor

A classe `DailyInformProcessor` é a classe responsável por realizar operações de transformação no objeto `DailyInform`. É neste estágio que pode-se fazer as mais completas operações tais como aplicar cálculos, fazer formatações, agregar dados e até mesmo converter a saída para um outro objeto completamente diferente. No caso deste tutorial, para ilustrar o seu uso eu utilizei este passo para formatar o valor do CNPJ das empresas ao remover pontos e traços. Para isso, peguei uma ajudinha na biblioteca da [Alura Stella](http://stella.caelum.com.br/), que contém diversas facilidades para manipular documentos brazucas. Além disso, não modifiquei o tipo de objeto de saída (entra DailyInform, sai DailyInform). A classe tem implementação simples e ficou da seguinte maneira:

```java
@Component
public class DailyInformProcessor implements ItemProcessor<DailyInform, DailyInform> {

   @Autowired
   private CNPJFormatter cnpjFormatter;

   @Override
   public DailyInform process(DailyInform dailyInform) throws Exception {
       dailyInform.setCnpj(cnpjFormatter.unformat(dailyInform.getCnpj()));
       return dailyInform;
   }
}

```

Observe que é necessário implementar o método `process` da interface `ItemProcessor`, bem auto explicativo.

Por último só faltou implementar o estágio da escrita.

## Parte 4 - Configurando a Escrita

Primeiramente, vamos criar um repositório para a entidade `DailyInform` que será utilizado pelo `Writer` para escrever os dados no DB.

```java
@Repository
public interface DailyInformRepository extends JpaRepository<DailyInform, Long> {
   List<DailyInform> findDistinctByCnpj(String cnpj);
}

```

É um repositório simples utilizando Spring Data sem muito mistério. Só adicionei um método para achar os informes diários via CNPJ pois eu criei um endpoint na camada web que traz os resultados por empresa.

Feito o repositório, agora é só criar o `Writer`, que basicamente é um componente Spring que implementa a inteface `ItemWriter`:

```java
@Component
public class DBWriter implements ItemWriter<DailyInform> {

   private final DailyInformRepository dailyInformRepository;

   @Autowired
   public DBWriter(DailyInformRepository dailyInformRepository) {
       this.dailyInformRepository = dailyInformRepository;
   }

   @Override
   public void write(List<? extends DailyInform> list) {
       dailyInformRepository.saveAll(list);
   }
}

```

O único método que o a interface impõe implementar o é o `write`. Este por sua vez recebe uma lista com dados (os chunks que são transformados no passo anterior), que por sua vez eu mando escrever no banco utilizando o `DailyInformRepository`, injetado aqui na classe como um atributo chamado `dailyInformRepository`. As JpaRepositories possuem um método chamado `saveAll` onde se passa uma collection Java que será salva no banco, feito isso aquelas informações são persistidas e nossos dados finalmente chegam ao seu destino final =D

## Bônus - Camada Web

Uma vez que os dados foram salvos no banco de dados, é bem tranquilo fazer uma camada web para expor as informações em formato JSON. Se você é bem tradicional e quer seguir a arquitetura N-camadas, pode fazer uma Service um Controller. Os trechos de código abaixo ilustram como fazer isso:

```java
@Service
public class FundsService {

   private final DailyInformRepository dailyInformRepository;

   @Autowired
   public FundsService(DailyInformRepository dailyInformRepository) {
       this.dailyInformRepository = dailyInformRepository;
   }

   @Transactional(readOnly = true)
   public List<DailyInform> getDailyInformByCNPJ(String cnpj) {
       return dailyInformRepository.findDistinctByCnpj(cnpj);
   }
}
```

E a controller:

```java
@RestController
@RequestMapping("/api/daily-informs")
public class FundsController {

   private final FundsService fundsService;

   @Autowired
   public FundsController(FundsService fundsService) {
       this.fundsService = fundsService;
   }

   @GetMapping("/{cnpj}")
   public ResponseEntity<List<DailyInform>> all(@PathVariable("cnpj") String cnpj) {
       return ResponseEntity.of(Optional.of(fundsService.getDailyInformByCNPJ(cnpj)));
   }
}

```

Sobre estes dois componentes tenho as seguintes considerações:

1. Repare que eu injeto o repositório no serviço, e o serviço na controller. Seguindo os preceitos de arquitetura em camadas e expondo a camada de dados somente via serviços.
2. Em `FundsService` eu coloquei a annotation `@Transaction` com o parâmetro readonly para true, desta forma eu informo para o Spring abrir um contexto transacional deste método e que não vou fazer operações de escrita e remoção no banco de dados, unindo controle transacional com um ganho de performance.
3. Na controller resolvi utilizar o cnpj como um `PathParam`, mas existem outras formas que isso pode ser feito (através de uma queryParam obrigatória por exemplo).
4. Por fim eu injetei todos os componentes através de construtores de classe, gosto de fazer assim pois consigo declarar minhas dependências como final, deixando os atributos da classe mais imutáveis.

## Botanto para Rodar!

Feito tudo agora chegou a hora de rodar esse batch. Pra rodar o job assim que o Spring startar eu implementei a interface CommandLineRunner na classe principal que executa o job configurado. O resultado fica assim:

```java
@SpringBootApplication
public class BatchProcessingApplication implements CommandLineRunner {

   @Bean
   public CNPJFormatter cnpjFormatter() {
       return new CNPJFormatter();
   }

   @Autowired
   private JobLauncher jobLauncher;

   @Autowired
   private Job job;

   public static void main(String[] args) {
       SpringApplication.run(BatchProcessingApplication.class, args);
   }

   @Override
   public void run(String... args) throws Exception {
       jobLauncher.run(job, new JobParameters());
   }
}

```

O bean `Job` é detectado pelo Spring e ele vem lá do `Job` que criamos no arquivo de configuração, além disso, precisamos de um `JobLaucher` que nada mais é um do que um bean provido pelo Spring para fazer o lançamento do seu job. Fazemos isso colocando o atributo job dentro do método run do `jobLauncher`. O parâmetro `JobParameters` é basicamente um Wrapper de um `Map<String, Object>` que te permite passar parâmetros para o job que vai rodar (não vamos utilizar isto neste tutorial, logo podemos passar uma instância vazia).

Fazendo isso dando play (certifque-se que os seu banco de dados está de pé e acessível da sua aplicação) podemos ver os jobs rodando…..

E aqui na minha máquina demorou mais ou menos uma hora e meia para processar as 200 mil linhas de dados.

[![Resultados Cmd](https://i.imgur.com/gIyfW6P.png "Resultados em execução")](https://i.imgur.com/gIyfW6P.png){:data-lightbox="Resultados Cmd"}{:data-title="Resultados depois do processamento dos lotes"}

O banco de dados ficou assim:

[![Resultados BD](https://i.imgur.com/jf34chM.png "Resultados no banco")](https://i.imgur.com/jf34chM.png){:data-lightbox="Resultados BD"}{:data-title="Informações carregadas no banco de dados"}

E este é o resultado usando o endpoint que criei pra trazer os resultados por CNPJ, bacana não ? XD

[![Resultados JSON](https://i.imgur.com/XmSLgAA.png "Resultados JSON")](https://i.imgur.com/XmSLgAA.png){:data-lightbox="Resultados JSON"}{:data-title="Os dados no banco servidos em JSON"}

## Conclusão

Wow se você chegou até aqui meus parabéns! Hehehe este tutorial ficou bem maior do que eu esperava, mas tá ótimo pra começar este blog com o pé direito. É óbvio que tem bastante coisa pra melhorar, mas agora eu tenho um norte e espero fazer pelo menos 2 artigos novos aqui por mês.

Quanto a solução, bom funcionou, mas eu creio que existem bastante pontos de melhoria. Algumas coisas eu já descobri que dá pra melhorar e posso falar um pouco mais em um próximo artigo. Um exemplo é que se houver alguma linha com informações irregulares na planilha, todo o job para. Como isso não é desejável, precisamos definir uma política de tolerância e registro de erros. Outra coisa que também é possível fazer é paralelizar a execução, como deixar o processamento de forma mais eficiente ? Será que tem que fazer alguma configuração a mais no banco ? Fique ligado para mais conteúdo vindo por aí.

Espero que tenham gostado deste meu primeiro artigo e peço pra que você compartilhe com os seus colegas. Sinta-se livre para fazer observações nos comentários, quanto mais feedback receber melhor!

[GitHub da solução](https://github.com/lcastrooliveira/funds_daily_report)

Abraços;