## PPL `rename` command

### Description
Using ``rename`` command to rename one or more fields in the search result.


### Syntax
`rename <source-field> AS <target-field>["," <source-field> AS <target-field>]...`

* source-field: mandatory. The name of the field you want to rename.
* field list: mandatory. The name you want to rename to.


### Example 1: Rename one field

The example show rename one field.

PPL query:

    os> source=accounts | rename account_number as an | fields an;
    fetched rows / total rows = 4/4
    +------+
    | an   |
    |------|
    | 1    |
    | 6    |
    | 13   |
    | 18   |
    +------+


### Example 2: Rename multiple fields

The example show rename multiple fields.

PPL query:

    os> source=accounts | rename account_number as an, employer as emp | fields an, emp;
    fetched rows / total rows = 4/4
    +------+---------+
    | an   | emp     |
    |------+---------|
    | 1    | Pyrami  |
    | 6    | Netagy  |
    | 13   | Quility |
    | 18   | null    |
    +------+---------+

### Limitation:
- `rename` command needs spark version >= 3.4

- Overriding existing field is unsupported:

`source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address`
