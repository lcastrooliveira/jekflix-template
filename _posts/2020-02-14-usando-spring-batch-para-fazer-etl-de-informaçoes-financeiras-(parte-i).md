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

Na minha busca por um caso de uso bacana para entender um pouco mais o Spring Batch,  e aproveitando que ano passado estava inserido no Grupo de Estudos em Ciência de Dados, um projeto de extensão da Unila que você pode conferir mais clicando neste link, me deparei com o conceito de ETL.

## ELT (TL;DR)

Um ETL é uma sigla inglesa que significa Extract Transform e Load (Extrair, Transformar e “Carregar”). A idéia em si é muito simples e consiste em operações que envolvem grande volumes de dados compostas em basicamente em três fases: extração, transformação e carregamento (ou armazenamento).

Seu uso é muito comum quando se quer transferir um ou até mesmo vários dados de um conjunto de armazenamento para o outro. Sendo que entre origem e destino é possível fazer transformações nos dados tais como agregação, formatação e entre outros.

Descrição do problema que eu fui caçar:

Com este conceito em mente, precisava de um conjunto de dados para começar. Como eu me amarro em aprender mais sobre produtos financeiros, tais como renda fixa, ações e fundos imobiliários fui procurar alguma coisa que envolvesse esses tipos de dados. Descobri que a CVM (Comissão de Valores Imobiliários) possui um portal de dados abertos(fazer link) onde é possível baixar as mais diversas planilhas relacionadas a fundos de investimento tais como informe diário, informações cadastrais e entre outros.

Dentre os conjuntos de dados fornecidos pela CVM existe um denominado “Informe diário” que segundo a própria CVM tem um demonstrativo com informações como:

* Valor total da carteira do fundo
* Patrimônio líquido
* Valor da cota
* Captações realizadas no dia
* Resgates pagos no dia
* Número de cotistas

Baixei uma planilha de um dia aleatório e boom! Um csv com mais de 200.000 linhas de dados estruturados. Perfeito para arregaçar as mangas e começar a brincar.

Dentre as dezenas de colunas disponibilizadas na planilha, elegi algumas para utilizar na minha solução de ETL, são elas:

1. CNPJ da empresa emissora
2. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.
3. Maecenas sed diam eget risus varius blandit sit amet non magna.

Como primeiro passo pensei em só fazer um ETL simples, ou seja carregar as informações do CSV e gravá-las em um banco de dados MySQL. Para não ficar sem fazer nenhuma mudança no passo de transformação, resolvi apenas remover a formatação do campo CNPJ, ou seja, no banco eu somente salvo os dígitos, sem pontos ou traços.

O rascunho de guardanapo da minha solução ficou assim:

A ideia é, ler o arquivo CSV, fazer um ETL usando Spring Batch, gravar as informações em uma tabela no banco de dados usando o Spring Data, e finamente utilizar o Spring Web para disponibilizar as informações em formato JSON através de uma API.

O esquema de dados da entidade que eu criei no banco ficou assim:

Apresentados a ideia e uma arquitetura geral do brincadeira, bora escrever um pouco de código!

## Passo 1, fazendo meu “Hello World” com Spring Batch

Para utilizar o Spring Batch primeiro você precisa colocar as dependências dele no seu projeto. Você pode começar criando um projeto no Spring Initializr selecionando as seguintes dependências:

* Spring boot starter data jpa: (para fazer conexão ao banco de dados)
* Spring boot starter web: (para criar a camada web responsável por expor os dados vai uma API REST com JSON).
* mySql-connector-java: Para este tutorial eu optei por utilizar o MySQL, mas qualquer outro DB relacional funciona.
* spring-boot-starter-batch: A estrelinha do projeto, contém as classes necessárias para utilizar o Spring Batch.

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

   public DailyInform(String cnpj, LocalDate referenceDate, BigDecimal totalValue, BigDecimal quotaValue, BigDecimal netWorth, BigDecimal totalDeposits, BigDecimal totalWithdrawals, Long numberOfQuotaHolders) {
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

Os processamentos em lote no Spring são executados através de um Job. Um job é composto de Steps que definem passos sucessivos de leitura, transformação e escrita que um Job pode ter (para este tutorial faremos um Job com um único Step). O Step  basicamente tem três elementos:

1. Um reader: Define o que e como vai ser lido. Nesta fase que se define de onde virão os dados (no caso a planilha), e quais colunas serão digeridas.
2. Um processor: Define uma operação de transformação que o dado lido terá que sofrer até estar pronto para a escrita. Recebe um objeto de entrada e retorna outro objeto de saída.
3. Um writer: Define onde os dados recém processados serão escritos, neste artigo iremos utilizar um  Repository bean para escrever as informações na tabela do banco de dados.

Para utilizar um Job no SpringBatch é necessário configurá-lo. Segue um exemplo de um trecho do arquivo @Configuration(lembrando que o código completo deste tutorial encontra-se no meu GitHub:

```java
@Configuration
@EnableBatchProcessing
public class SpringBatchConfiguration {

   @Bean
   public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
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

Como pode ser visto na linha 25, é necessário utilizar a annotation @EnableBatchProcessing para habilitar o Spring Batch no projeto. O Job é exposto para aplicação através de um bean, sendo gerenciado pelo SpringContainer. O método job recebe cinco parâmetros: jobBuilderFactory, stepBuilderFactory, itemReader, itemProcessor, itemWriter. As factories são injetadas pelo próprio Spring, ao passo que cabe a nós definir os outros parâmetros através de Beans na aplicação. Uma vez definidos o próprio Spring os Injeta no método e cria o Job. Observe que os tipos do reader, processor e writer são tipados de acordo com a Entidade modelada DailyInform. É perfeitamente possível modelar um fluxo onde a leitura é de um tipo, convertido a outro no processamento e depois gravado na estágio de leitura. Mas para manter este tutorial mais simples vamos seguir com a idéia de manter o mesmo tipo durante todo o ciclo.

Graças a injeção de dependência, o spring consegue montar o Job utilizando um reader, processor e writer que também são Beans na aplicação. Vou explicar como eles são criados e o detalhamento de cada um deles nas sessões a seguir.

## Passo 2 - Criando o bean de leitura

Podemos criar um bean de Leitura a partir da classe FlatFileItemReader<T>, uma classe que implementa indiretamente a interface ItemReader<T>. A facilidade de usar esta classe é que ela foi projetada para cenários de leituras em arquivos linha a linha (perfeito para o nosso exemplo de CSV). A sua configuração é relativamente simples, no entanto ela exige a criação de outro bean auxiliar. Segue um exemplo da implementação:

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

Entrada: Resource (uma referência ao arquivo .csv), aqui eu setei o path em uma variável de ambiente ${input} por conveniência.

Saída: Um objeto do tipo `FlatFileItemReader<DailyInform>` onde eu configuro:

* O recurso que será lido
* O encoding do arquivo, no caso eu descobri que a planilha baixada não está em UTF-8, logo eu precisei informar o encoding correto para não haver falhas de leitura de * caracteres especiais tais como à ó ã (Português né ?)
* Nome deste bean: provavelmente utilizado pelo Spring para uma indexação interna deste reader.
* Linhas para pular: Coloquei o valor 1 para pular o cabeçalho e ir direto para os dados.
* O LineMapper: O bean auxiliar necessário para configurar as políticas de leitura. Sua criação será detalhada a seguir.

### Criando o LineMapper

O LineMapper é o bean que basicamente define qual vai ser a lógica de leitura tais como: Qual caractere é o delimitador de colunas, qual é o nome das colunas e como converter a linha lida em objeto. Sua construção se dá da seguinte forma:

```java
@Bean
public LineMapper<DailyInform> lineMapper() {
   DefaultLineMapper<DailyInform> defaultLineMapper = new DefaultLineMapper<>();
   DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

   lineTokenizer.setDelimiter(";");

   lineTokenizer.setNames("CNPJ_FUNDO", "DT_COMPTC", "VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST");
   lineTokenizer.setStrict(false);

   DailyInformFieldSetMapper dailyInformFieldSetMapper = new DailyInformFieldSetMapper();

   defaultLineMapper.setLineTokenizer(lineTokenizer);
   defaultLineMapper.setFieldSetMapper(dailyInformFieldSetMapper);

   return defaultLineMapper;
}

```

O objeto retornado por este método é uma instância da classe DefaultLineMapper, que implementa a interface LineMapper, onde é configurado:

* O delimitador: no caso do CSV da CVM foi utilizado o caractere ‘;’ para separar os dados de cada coluna
* Define os tokens (ou os nomes) de cada coluna que será lida, bem como a quantas serão processadas. Mais tarde estes nomes serão utilizados no bean que coloca as informações de cada linha em um objeto POJO DailyInform.
* Setar leitura estrita: Caso seja true toda linha lida deve ter o número correto de colunas, se for marcada como false o SpringBatch aceita a leitura de linhas que não possui o mesmo número de colunas definidos nos tokens. Caso haja menos colunas, os valores que faltam serão preenchidos com empty(vazio), caso haja mais colunas elas serão simplesmente ignoradas.
* O fieldsetmapper: Bean que define como os dados lidos das linhas serão convertidos em um POJO. Neste caso eu criei uma classe chamada DailyInformFieldSetMapper que faz este trabalho.

### O DailyInformFieldSetMapper (pois aqui não existe mágica)

Esta classe implementa a interface FieldSetMapper<T>, que por contrato obriga a implementar o método <T> mapFieldSet(FieldSet fieldSet), este método recebe por parâmetro uma linha lida pelo LineMapper e é nele que se implementa a lógica de colocar os tokens definidos em atributos da classe DailyInform. Esse é o coração da conversão do dado que está em uma linha CSV para um objeto Java completo (por isso que eu digo que aqui não existe mágica xD). A classe só tem o méotodo mapFieldSet e fica desta forma:

```java
public class DailyInformFieldSetMapper implements FieldSetMapper<DailyInform> {

   @Override
   public DailyInform mapFieldSet(FieldSet fieldSet) throws BindException {
       final DailyInform dailyInform = new DailyInform();
       dailyInform.setCnpj(fieldSet.readString("CNPJ_FUNDO"));
       dailyInform.setReferenceDate(fieldSet.readDate("DT_COMPTC").toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
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

Feito isso, tudo está pronto e configurado para o passo de Leitura, agora só falta os passos de transformação e escrita, mas felizmente eles são bem mais simples de fazer conforme eu vou mostrar agora.

## Parte 3 - Configurando o Processor

A classe DailyInformProcessor é a classe responsável por realizar operações de transformação no Objeto DailyInform. É neste estágio que pode-se fazer as mais completas operações tais como aplicar cálculos, fazer formatações, agregar dados e até mesmo converter a saída para um outro objeto completamente diferente., No caso deste tutorial, para ilustrar o seu uso eu utilizei este passo para formatar o valor do CNPJ das empresas ao remover pontos e traços. Para isso, peguei uma ajudinha na biblioteca da Alura Stella(link), que contém diversas facilidades para manipular documentos brazucas. Além disso, não modifiquei o tipo de objeto de saída (entra DailyInform, sai DailyInform). A classe tem implementação simples e ficou da seguinte maneira:

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

Observe que é necessário implementar o método process da interface ItemProcessor, bem auto explicativo.

Por último só faltou implementar o estágio da escrita. É o que vou mostrar agora.


Cas sociis natoque penatibus et magnis <a href="#">dis parturient montes</a>, nascetur ridiculus mus. *Aenean eu leo quam.* Pellentesque ornare sem lacinia quam venenatis vestibulum. Sed posuere consectetur est at lobortis. Cras mattis consectetur purus sit amet fermentum.

> Curabitur blandit tempus porttitor. Nullam quis risus eget urna mollis ornare vel eu leo. Nullam id dolor id nibh ultricies vehicula ut id elit.

Etiam porta **sem malesuada magna** mollis euismod. Cras mattis consectetur purus sit amet fermentum. Aenean lacinia bibendum nulla sed consectetur.

## Inline HTML elements

HTML defines a long list of available inline tags, a complete list of which can be found on the [Mozilla Developer Network](https://developer.mozilla.org/en-US/docs/Web/HTML/Element).

- **To bold text**, use `<strong>`.
- *To italicize text*, use `<em>`.
- Abbreviations, like <abbr title="HyperText Markup Langage">HTML</abbr> should use `<abbr>`, with an optional `title` attribute for the full phrase.
- Citations, like <cite>&mdash; Thiago Rossener</cite>, should use `<cite>`.
- <del>Deleted</del> text should use `<del>` and <ins>inserted</ins> text should use `<ins>`.
- Superscript <sup>text</sup> uses `<sup>` and subscript <sub>text</sub> uses `<sub>`.

Most of these elements are styled by browsers with few modifications on our part.

# Heading 1

## Heading 2

### Heading 3

#### Heading 4

Vivamus sagittis lacus vel augue rutrum faucibus dolor auctor. Duis mollis, est non commodo luctus, nisi erat porttitor ligula, eget lacinia odio sem nec elit. Morbi leo risus, porta ac consectetur ac, vestibulum at eros.

## Code

Cum sociis natoque penatibus et magnis dis `code element` montes, nascetur ridiculus mus.

```js
// Example can be run directly in your JavaScript console

// Create a function that takes two arguments and returns the sum of those arguments
var adder = new Function("a", "b", "return a + b");

// Call the function
adder(2, 6);
// > 8
```

Aenean lacinia bibendum nulla sed consectetur. Etiam porta sem malesuada magna mollis euismod. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa.

## Lists

Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Aenean lacinia bibendum nulla sed consectetur. Etiam porta sem malesuada magna mollis euismod. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus.

* Praesent commodo cursus magna, vel scelerisque nisl consectetur et.
* Donec id elit non mi porta gravida at eget metus.
* Nulla vitae elit libero, a pharetra augue.

Donec ullamcorper nulla non metus auctor fringilla. Nulla vitae elit libero, a pharetra augue.

1. Vestibulum id ligula porta felis euismod semper.
2. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.
3. Maecenas sed diam eget risus varius blandit sit amet non magna.

Cras mattis consectetur purus sit amet fermentum. Sed posuere consectetur est at lobortis.

Integer posuere erat a ante venenatis dapibus posuere velit aliquet. Morbi leo risus, porta ac consectetur ac, vestibulum at eros. Nullam quis risus eget urna mollis ornare vel eu leo.

## Images

Quisque consequat sapien eget quam rhoncus, sit amet laoreet diam tempus. Aliquam aliquam metus erat, a pulvinar turpis suscipit at.

![placeholder](https://placehold.it/800x400 "Large example image")
![placeholder](https://placehold.it/400x200 "Medium example image")
![placeholder](https://placehold.it/200x200 "Small example image")

## Tables

Aenean lacinia bibendum nulla sed consectetur. Lorem ipsum dolor sit amet, consectetur adipiscing elit.

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Upvotes</th>
      <th>Downvotes</th>
    </tr>
  </thead>
  <tfoot>
    <tr>
      <td>Totals</td>
      <td>21</td>
      <td>23</td>
    </tr>
  </tfoot>
  <tbody>
    <tr>
      <td>Alice</td>
      <td>10</td>
      <td>11</td>
    </tr>
    <tr>
      <td>Bob</td>
      <td>4</td>
      <td>3</td>
    </tr>
    <tr>
      <td>Charlie</td>
      <td>7</td>
      <td>9</td>
    </tr>
  </tbody>
</table>

Nullam id dolor id nibh ultricies vehicula ut id elit. Sed posuere consectetur est at lobortis. Nullam quis risus eget urna mollis ornare vel eu leo.