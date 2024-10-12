<?php

namespace LessQL;

/**
 * Database object wrapping a PDO instance
 */
class Database
{
    /** @var string */
    protected string $identifierDelimiter = "`";

    /** @var array */
    protected array $primary = [];

    /** @var array */
    protected array $references = [];

    /** @var array */
    protected array $backReferences = [];

    /** @var array */
    protected array $aliases = [];

    /** @var array */
    protected array $required = [];

    /** @var array */
    protected array $sequences = [];

    /** @var null|callable */
    protected $rewrite = null;

    /** @var null|callable */
    protected $queryCallback = null;

    /**
     * Constructor. Sets PDO to exception mode.
     *
     * @param \PDO $pdo
     */
    public function __construct(protected \Pdo $pdo)
    {
        // required for safety
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * Returns a result for table $name.
     * If $id is given, return the row with that id.
     *
     * Examples:
     * $db->user()->where( ... )
     * $db->user( 1 )
     *
     * @param string $name
     * @param array $args
     * @return Result|Row|null
     */
    public function __call(string $name, array $args) : Result|Row|null
    {
        array_unshift($args, $name);

        return call_user_func_array(array($this, 'table'), $args);
    }

    /**
     * Returns a result for table $name.
     * If $id is given, return the row with that id.
     *
     * @param $name
     * @param int|null $id
     * @return Result|Row|null
     */
    public function table(string $name, ?int $id = null) : Result|Row|null
    {
        // ignore List suffix
        $name = preg_replace('/List$/', '', $name);

        if ($id !== null) {
            $result = $this->createResult($this, $name);

            if (!is_array($id)) {
                $table = $this->getAlias($name);
                $primary = $this->getPrimary($table);
                $id = array($primary => $id);
            }

            return $result->where($id)->fetch();
        }

        return $this->createResult($this, $name);
    }

    // Factories

    /**
     * Create a row from given properties.
     * Optionally bind it to the given result.
     *
     * @param string $name
     * @param array $properties
     * @param Result|null $result
     * @return Row
     */
    public function createRow(string $name, array $properties = [], ?Result $result = null) : Row
    {
        return new Row($this, $name, $properties, $result);
    }

    /**
     * Create a result bound to $parent using table or association $name.
     * $parent may be the database, a result, or a row
     *
     * @param Database|Result|Row $parent
     * @param string $name
     * @return Result
     */
    public function createResult(Database|Result|Row $parent, string $name) : Result
    {
        return new Result($parent, $name);
    }

    // PDO interface

    /**
     * Prepare an SQL statement
     *
     * @param string $query
     * @return \PDOStatement
     */
    public function prepare(string $query) : \PDOStatement
    {
        return $this->pdo->prepare($query);
    }

    /**
     * Execute an SQL statement directly
     *
     * @param string $query
     * @return \PDOStatement
     */
    public function query(string $query) : \PDOStatement
    {
        return $this->pdo->query($query);
    }

    /**
     * Return last inserted id
     *
     * @param string|null $sequence
     * @return string|false
     */
    public function lastInsertId(?string $sequence = null) : string|false
    {
        return $this->pdo->lastInsertId($sequence);
    }

    /**
     * Begin a transaction
     *
     * @return bool
     */
    public function begin() : bool
    {
        return $this->pdo->beginTransaction();
    }

    /**
     * Commit changes of transaction
     *
     * @return bool
     */
    public function commit() : bool
    {
        return $this->pdo->commit();
    }

    /**
     * Rollback any changes during transaction
     *
     * @return bool
     */
    public function rollback() : bool
    {
        return $this->pdo->rollBack();
    }

    // Schema hints

    /**
     * Get primary key of a table, may be array for compound keys
     *
     * Convention is "id"
     *
     * @param string $table
     * @return string|array
     */
    public function getPrimary(string $table) : string|array
    {
        if (isset($this->primary[$table])) {
            return $this->primary[$table];
        }

        return 'id';
    }

    /**
     * Set primary key of a table.
     * Compound keys may be passed as an array.
     * Always set compound primary keys explicitly with this method.
     *
     * @param string $table
     * @param string|array $key
     * @return $this
     */
    public function setPrimary(string $table, string|array $key) : self
    {
        $this->primary[$table] = $key;

        // compound keys are never auto-generated,
        // so we can assume they are required
        if (is_array($key)) {
            foreach ($key as $k) {
                $this->setRequired($table, $k);
            }
        }

        return $this;
    }

    /**
     * Get a reference key for an association on a table
     *
     * "How would $table reference another table under $name?"
     *
     * Convention is "$name_id"
     *
     * @param string $table
     * @param string $name
     * @return string
     */
    public function getReference(string $table, string $name) : string
    {
        if (isset($this->references[$table][$name])) {
            return $this->references[$table][$name];
        }

        return $name . '_id';
    }

    /**
     * Set a reference key for an association on a table
     *
     * @param string $table
     * @param string $name
     * @param string $key
     * @return $this
     */
    public function setReference(string $table, string $name, string $key) : self
    {
        $this->references[$table][$name] = $key;

        return $this;
    }

    /**
     * Get a back reference key for an association on a table
     *
     * "How would $table be referenced by another table under $name?"
     *
     * Convention is "$table_id"
     *
     * @param string $table
     * @param string $name
     * @return string
     */
    public function getBackReference(string $table, string $name) : string
    {
        if (isset($this->backReferences[$table][$name])) {
            return $this->backReferences[$table][$name];
        }

        return $table . '_id';
    }

    /**
     * Set a back reference key for an association on a table
     *
     * @param string $table
     * @param string $name
     * @param string $key
     * @return $this
     */
    public function setBackReference(string $table, string $name, string $key) : self
    {
        $this->backReferences[$table][$name] = $key;

        return $this;
    }

    /**
     * Get alias of a table
     *
     * @param string $alias
     * @return string
     */
    public function getAlias(string $alias) : string
    {
        return isset($this->aliases[$alias]) ? $this->aliases[$alias] : $alias;
    }

    /**
     * Set alias of a table
     *
     * @param string $alias
     * @param string $table
     * @return $this
     */
    public function setAlias(string $alias, string $table) : self
    {
        $this->aliases[$alias] = $table;

        return $this;
    }

    /**
     * Is a column of a table required for saving? Default is no
     *
     * @param string $table
     * @param string $column
     * @return bool
     */
    public function isRequired(string $table, string $column) : bool
    {
        return isset($this->required[$table][$column]);
    }

    /**
     * Get a map of required columns of a table
     *
     * @param string $table
     * @return array
     */
    public function getRequired(string $table) : array
    {
        return isset($this->required[$table]) ? $this->required[$table] : [];
    }

    /**
     * Set a column to be required for saving
     * Any primary key that is not auto-generated should be required
     * Compound primary keys are required by default
     *
     * @param string $table
     * @param string $column
     * @return $this
     */
    public function setRequired(string $table, string $column) : self
    {
        $this->required[$table][$column] = true;

        return $this;
    }

    /**
     * Get primary sequence name of table (used in INSERT by Postgres)
     *
     * Conventions is "$tableRewritten_$primary_seq"
     *
     * @param string $table
     * @return null|string
     */
    public function getSequence(string $table) : ?string
    {
        if (isset($this->sequences[$table])) {
            return $this->sequences[$table];
        }

        $primary = $this->getPrimary($table);

        if (is_array($primary)) {
            return null;
        }

        $table = $this->rewriteTable($table);

        return $table . '_' . $primary . '_seq';
    }

    /**
     * Set primary sequence name of table
     *
     * @param string $table
     * @param string $sequence
     * @return $this
     */
    public function setSequence(string $table, string $sequence) : self
    {
        $this->sequences[$table] = $sequence;

        return $this;
    }

    /**
     * Get rewritten table name
     *
     * @param string $table
     * @return string
     */
    public function rewriteTable(string $table) : string
    {
        if (is_callable($this->rewrite)) {
            return call_user_func($this->rewrite, $table);
        }

        return $table;
    }

    /**
     * Set table rewrite function
     * For example, it could add a prefix
     *
     * @param callable $rewrite
     * @return $this
     */
    public function setRewrite(callable $rewrite) : self
    {
        $this->rewrite = $rewrite;

        return $this;
    }

    // SQL style

    /**
     * Get identifier delimiter
     *
     * @return string
     */
    public function getIdentifierDelimiter() : string
    {
        return $this->identifierDelimiter;
    }

    /**
     * Sets delimiter used when quoting identifiers.
     * Should be backtick or double quote.
     * Set to null to disable quoting.
     *
     * @param string|null $d
     * @return $this
     */
    public function setIdentifierDelimiter(?string $d) : self
    {
        $this->identifierDelimiter = $d;

        return $this;
    }

    // Queries

    /**
     * Select rows from a table
     *
     * @param string $table
     * @param mixed $exprs
     * @param array $where
     * @param array $orderBy
     * @param int|null $limitCount
     * @param int|null $limitOffset
     * @param array $params
     * @return \PDOStatement
     */
    public function select(string $table, array $options = []) : \PDOStatement
    {
        $options = array_merge([
            'expr' => null,
            'where' => [],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [],
            'limitCount' => null,
            'limitOffset' => null,
            'params' => [],
        ], $options);

        $query = "SELECT ";

        if (empty($options['expr'])) {
            $query .= "*";
        } elseif (is_array($options['expr'])) {
            $query .= implode(", ", $options['expr']);
        } else {
            $query .= $options['expr'];
        }

        $table = $this->rewriteTable($table);
        $query .= " FROM " . $this->quoteIdentifier($table);

        $query .= $this->getSuffix($options['where'], $options['groupBy'], $options['having'], $options['orderBy'], $options['limitCount'], $options['limitOffset']);

        $this->onQuery($query, $options['params']);

        $statement = $this->prepare($query);
        $statement->setFetchMode(\PDO::FETCH_ASSOC);
        $statement->execute($options['params']);

        return $statement;
    }

    /**
     * Insert one ore more rows into a table
     *
     * The $method parameter selects one of the following insert methods:
     *
     * "prepared": Prepare a query and execute it once per row using bound params
     *             Does not support Literals in row data (PDO limitation)
     *
     * "batch":    Create a single query mit multiple value lists
     *             Supports Literals, but not supported everywhere
     *
     * default:    Execute one INSERT per row
     *             Supports Literals, supported everywhere, slow for many rows
     *
     * @param string $table
     * @param array $rows
     * @param string|null $method
     * @return \PDOStatement|null
     */
    public function insert(string $table, array $rows, ?string $method = null) : ?\PDOStatement
    {
        if (empty($rows)) {
            return null;
        }
        if (!isset($rows[0])) {
            $rows = array($rows);
        }

        if ($method === 'prepared') {
            return $this->insertPrepared($table, $rows);
        } elseif ($method === 'batch') {
            return $this->insertBatch($table, $rows);
        }

        return $this->insertDefault($table, $rows);
    }

    /**
     * Insert rows using a prepared query
     *
     * @param string $table
     * @param array $rows
     * @return \PDOStatement|null
     */
    protected function insertPrepared(string $table, array $rows) : ?\PDOStatement
    {
        $columns = $this->getColumns($rows);
        if (empty($columns)) {
            return null;
        }

        $query = $this->insertHead($table, $columns);
        $query .= "( ?" . str_repeat(", ?", count($columns) - 1) . " )";

        $statement = $this->prepare($query);

        foreach ($rows as $row) {
            $values = array();

            foreach ($columns as $column) {
                $value = (string) $this->format(@$row[$column]);
                $values[] = $value;
            }

            $this->onQuery($query, $values);

            $statement->execute($values);
        }

        return $statement;
    }

    /**
     * Insert rows using a single batch query
     *
     * @param string $table
     * @param array $rows
     * @return \PDOStatement|null
     */
    protected function insertBatch(string $table, array $rows) : ?\PDOStatement
    {
        $columns = $this->getColumns($rows);
        if (empty($columns)) {
            return null;
        }

        $query = $this->insertHead($table, $columns);
        $lists = $this->valueLists($rows, $columns);
        $query .= implode(", ", $lists);

        $this->onQuery($query);

        $statement = $this->prepare($query);
        $statement->execute();

        return $statement;
    }

    /**
     * Insert rows using one query per row
     *
     * @param string $table
     * @param array $rows
     * @return \PDOStatement|null
     */
    protected function insertDefault(string $table, array $rows) : ?\PDOStatement
    {
        $columns = $this->getColumns($rows);
        if (empty($columns)) {
            return null;
        }

        $query = $this->insertHead($table, $columns);
        $lists = $this->valueLists($rows, $columns);

        foreach ($lists as $list) {
            $singleQuery = $query . $list;

            $this->onQuery($singleQuery);

            $statement = $this->prepare($singleQuery);
            $statement->execute();
        }

        return $statement; // last statement is returned
    }

    /**
     * Build head of INSERT query (without values)
     *
     * @param string $table
     * @param array $columns
     * @return string
     */
    protected function insertHead(string $table, array $columns) : string
    {
        $quotedColumns = array_map(array($this, 'quoteIdentifier' ), $columns);
        $table = $this->rewriteTable($table);
        $query = "INSERT INTO " . $this->quoteIdentifier($table);
        $query .= " ( " . implode(", ", $quotedColumns) . " ) VALUES ";

        return $query;
    }

    /**
     * Get list of all columns used in the given rows
     *
     * @param array $rows
     * @return array
     */
    protected function getColumns(array $rows) : array
    {
        $columns = array();

        foreach ($rows as $row) {
            foreach ($row as $column => $value) {
                $columns[$column] = true;
            }
        }

        return array_keys($columns);
    }

    /**
     * Build lists of quoted values for INSERT
     *
     * @param array $rows
     * @param array $columns
     * @return array
     */
    protected function valueLists(array $rows, array $columns) : array
    {
        $lists = array();

        foreach ($rows as $row) {
            $values = array();

            foreach ($columns as $column) {
                $values[] = $this->quote(@$row[$column]);
            }

            $lists[] = "( " . implode(", ", $values) . " )";
        }

        return $lists;
    }

    /**
     * Execute update query and return statement
     *
     * UPDATE $table SET $data [WHERE $where]
     *
     * @param string $table
     * @param array $data
     * @param array|null $where
     * @param array|null $params
     * @return null|\PDOStatement
     */
    public function update(string $table, array $data, ?array $where = [], ?array $params = []) : ?\PDOStatement
    {
        if (empty($data)) {
            return null;
        }

        $set = [];

        foreach ($data as $column => $value) {
            $set[] = $this->quoteIdentifier($column) . " = " . $this->quote($value);
        }

        if (!is_array($where)) {
            $where = [$where];
        }
        if (!is_array($params)) {
            $params = array_slice(func_get_args(), 3);
        }

        $table = $this->rewriteTable($table);
        $query = "UPDATE " . $this->quoteIdentifier($table);
        $query .= " SET " . implode(", ", $set);
        $query .= $this->getSuffix($where);

        $this->onQuery($query, $params);

        $statement = $this->prepare($query);
        $statement->execute($params);

        return $statement;
    }

    /**
     * Execute delete query and return statement
     *
     * DELETE FROM $table [WHERE $where]
     *
     * @param string $table
     * @param array|null $where
     * @param array|null $params
     * @return \PDOStatement
     */
    public function delete(string $table, ?array $where = [], ?array $params = []) : \PDOStatement
    {
        if (!is_array($where)) {
            $where = [$where];
        }
        if (!is_array($params)) {
            $params = array_slice(func_get_args(), 2);
        }

        $table = $this->rewriteTable($table);
        $query = "DELETE FROM " . $this->quoteIdentifier($table);
        $query .= $this->getSuffix($where);

        $this->onQuery($query, $params);

        $statement = $this->prepare($query);
        $statement->execute($params);

        return $statement;
    }

    // SQL utility

    /**
     * Return WHERE/LIMIT/ORDER suffix for queries
     *
     * @param array $where
     * @param array $orderBy
     * @param int|null $limitCount
     * @param int|null $limitOffset
     * @return string
     */
    public function getSuffix(array $where, array $groupBy = [], array $having = [], array $orderBy = [], ?int $limitCount = null, ?int $limitOffset = null) : string
    {
        $suffix = "";

        if (!empty($where)) {
            $suffix .= " WHERE (" . implode(") AND (", $where).")";
        }

        if (!empty($groupBy)) {
            $suffix .= " GROUP BY " . implode(", ", $groupBy);
        }

        if (!empty($having)) {
            $suffix .= " HAVING (" . implode(") AND (", $having).")";
        }

        if (!empty($orderBy)) {
            $suffix .= " ORDER BY " . implode(", ", $orderBy);
        }

        if (isset($limitCount)) {
            $suffix .= " LIMIT " . intval($limitCount);

            if (isset($limitOffset)) {
                $suffix .= " OFFSET " . intval($limitOffset);
            }
        }

        return $suffix;
    }

    /**
     * Build an SQL condition expressing that "$column is $value",
     * or "$column is in $value" if $value is an array. Handles null
     * and literals like new Literal( "NOW()" ) correctly.
     *
     * @param string $column
     * @param mixed $value
     * @param bool $not
     * @return string
     */
    public function is(string $column, mixed $value, bool $not = false) : string
    {
        $bang = $not ? "!" : "";
        $or = $not ? " AND " : " OR ";
        $novalue = $not ? "1=1" : "0=1";
        $not = $not ? " NOT" : "";

        // always treat value as array
        if (!is_array($value)) {
            $value = [$value];
        }

        // always quote column identifier
        $column = $this->quoteIdentifier($column);

        if (count($value) === 1) {
            // use single column comparison if count is 1

            $value = $value[0];

            if ($value === null) {
                return $column . " IS" . $not . " NULL";
            } else {
                return $column . " " . $bang . "= " . $this->quote($value);
            }
        } elseif (count($value) > 1) {
            // if we have multiple values, use IN clause

            $values = [];
            $null = false;

            foreach ($value as $v) {
                if ($v === null) {
                    $null = true;
                } else {
                    $values[] = $this->quote($v);
                }
            }

            $clauses = [];

            if (!empty($values)) {
                $clauses[] = $column . $not . " IN ( " . implode(", ", $values) . " )";
            }

            if ($null) {
                $clauses[] = $column . " IS" . $not . " NULL";
            }

            return implode($or, $clauses);
        }

        return $novalue;
    }

    /**
     * Build an SQL condition expressing that "$column is not $value"
     * or "$column is not in $value" if $value is an array. Handles null
     * and literals like new Literal( "NOW()" ) correctly.
     *
     * @param string $column
     * @param mixed $value
     * @return string
     */
    public function isNot(string $column, mixed $value) : string
    {
        return $this->is($column, $value, true);
    }

    /**
     * Quote a value for SQL
     *
     * @param mixed $value
     * @return string
     */
    public function quote(mixed $value) : string|false
    {
        $value = $this->format($value);

        if ($value === null) {
            return "NULL";
        }

        if ($value === false) {
            return "'0'";
        }

        if ($value === true) {
            return "'1'";
        }

        if (is_int($value)) {
            return "'" . ((string) $value) . "'";
        }

        if (is_float($value)) {
            return "'" . sprintf("%F", $value) . "'";
        }

        if ($value instanceof Literal) {
            return $value->value;
        }

        return $this->pdo->quote($value);
    }

    /**
     * Format a value for SQL, e.g. DateTime objects
     *
     * @param mixed $value
     * @return mixed
     */
    public function format(mixed $value) : mixed
    {
        if ($value instanceof \DateTime) {
            return $value->format("Y-m-d H:i:s");
        }

        return $value;
    }

    /**
     * Quote identifier
     *
     * @param string $identifier
     * @return string
     */
    public function quoteIdentifier(string $identifier) : string
    {
        $delimiter = $this->identifierDelimiter;

        if (empty($delimiter)) {
            return $identifier;
        }

        $identifier = explode(".", $identifier);

        $identifier = array_map(
            function ($part) use ($delimiter) {
                return $delimiter . str_replace($delimiter, $delimiter.$delimiter, $part) . $delimiter;
            },
            $identifier
        );

        return implode(".", $identifier);
    }

    /**
     * Create a SQL Literal
     *
     * @param mixed $value
     * @return Literal
     */
    public function literal(mixed $value) : Literal
    {
        return new Literal((string) $value);
    }

    /**
     * Calls the query callback, if any
     *
     * @param string $query
     * @param array $params
     * @return void
     */
    public function onQuery(string $query, array $params = []) : void
    {
        if (is_callable($this->queryCallback)) {
            call_user_func($this->queryCallback, $query, $params);
        }
    }

    /**
     * Set the query callback
     *
     * @param callable $callback
     * @return $this
     */
    public function setQueryCallback(callable $callback) : self
    {
        $this->queryCallback = $callback;

        return $this;
    }
}
