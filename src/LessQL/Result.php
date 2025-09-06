<?php

namespace LessQL;

/**
 * Represents a filtered table result.
 *
 *  SELECT
 * 		{* | select_expr, ...}
 * 		FROM table
 * 			[WHERE condition [AND condition [...]]]
 * 			[ORDER BY {col_name | expr | position} [ASC | DESC], ...]
 * 			[LIMIT count [OFFSET offset]]
 *
 * TODO Add more SQL dialect specifics like FETCH FIRST, TOP etc.
 */
class Result implements \IteratorAggregate, \JsonSerializable
{
    // General members

    /** @var Database */
    protected Database $db;

    /** @var null|Row[] */
    protected ?array $rows = null;

    /** @var null|Row[] */
    protected ?array $globalRows = null;

    // Select information

    /** @var string */
    protected string $table;

    /** @var null|string */
    protected string|array|null $select = null;

    /** @var array */
    protected array $where = [];

    /** @var array */
    protected array $orWhere = [];

    /** @var array */
    protected array $whereParams = [];

    /** @var array */
    protected array $groupBy = [];

    /** @var array */
    protected array $having = [];

    /** @var array */
    protected array $orderBy = [];

    /** @var null|int */
    protected ?int $limitCount = null;

    /** @var null|int */
    protected ?int $limitOffset = null;

    // Members for results representing associations

    /** @var null|Result|Row */
    protected null|Result|Row $parent_ = null;

    /** @var null|bool */
    protected ?bool $single = null;

    /** @var null|string */
    protected ?string $key = null;

    /** @var null|string */
    protected ?string $parentKey = null;

    // Root members

    /** @var array */
    protected array $_cache = [];


    /**
     * Constructor
     * Use $db->createResult( $parent, $name ) instead
     *
     * @param Database|Result|Row $parent
     * @param string $name
     */
    public function __construct(Database|Result|Row $parent, string $name)
    {
        if ($parent instanceof Database) {

            // basic result

            $this->db = $parent;
            $this->table = $this->db->getAlias($name);
        } else { // Row or Result

            // result referenced to parent

            $this->parent_ = $parent;
            $this->db = $parent->getDatabase();

            // determine type of reference based on conventions and user hints

            $fullName = $name;
            $name = preg_replace('/List$/', '', $fullName);

            $this->table = $this->db->getAlias($name);

            $this->single = $name === $fullName;

            if ($this->single) {
                $this->key = $this->db->getPrimary($this->getTable());
                $this->parentKey = $this->db->getReference($parent->getTable(), $name);
            } else {
                $this->key = $this->db->getBackReference($parent->getTable(), $name);
                $this->parentKey = $this->db->getPrimary($parent->getTable());
            }
        }
    }

    /**
     * Get referenced row(s) by name. Suffix "List" gets many rows
     * Arguments are passed to where( $where, $params )
     *
     * @param string $name
     * @param array $args
     * @return Result
     */
    public function __call(string $name, array $args) : Result
    {
        array_unshift($args, $name);

        return call_user_func_array([$this, 'referenced'], $args);
    }

    /**
     * Get referenced row(s) by name. Suffix "List" gets many rows
     *
     * @param string $name
     * @param string|array|null $where
     * @param array $params
     * @return Result
     */
    public function referenced(string $name, string|array|null $where = null, array $params = []) : Result
    {
        $result = $this->db->createResult($this, $name);

        if ($where !== null) {
            if (!is_array($params)) {
                $params = array_slice(func_get_args(), 2);
            }
            $result = $result->where($where, $params);
        }

        return $result;
    }

    /**
     * Create result with new reference key
     *
     * @param string $key
     * @return Result
     * @throws \LogicException
     */
    public function via(string $key) : Result
    {
        if (!$this->parent_) {
            throw new \LogicException('Cannot set reference key on basic Result');
        }

        $clone = clone $this;

        if ($clone->single) {
            $clone->parentKey = $key;
        } else {
            $clone->key = $key;
        }

        return $clone;
    }

    /**
     * Execute the select query defined by this result.
     *
     * @return $this
     */
    public function execute() : self
    {
        if (isset($this->rows)) {
            return $this;
        }

        if ($this->parent_) {
            // restrict to parent
            $this->where[] = $this->db->is($this->key, $this->parent_->getGlobalKeys($this->parentKey));
        }

        $root = $this->getRoot();
        $definition = $this->getDefinition();

        $cached = $root->getCache($definition);

        if (!$cached) {
            // fetch all rows
            $statement = $this->db->select($this->table, [
                'expr' => $this->select,
                'where' => $this->where,
                'orWhere' => $this->orWhere,
                'groupBy' => $this->groupBy,
                'having' => $this->having,
                'orderBy' => $this->orderBy,
                'limitCount' => $this->limitCount,
                'limitOffset' => $this->limitOffset,
                'params' => $this->whereParams,
            ]);

            $rows = $statement->fetchAll();
            $cached = [];

            // build row objects
            foreach ($rows as $row) {
                $row = $this->createRow($row);
                $row->setClean();

                $cached[] = $row;
            }

            $root->setCache($definition, $cached);
        }

        $this->globalRows = $cached;

        if (!$this->parent_) {
            $this->rows = $cached;
        } else {
            $this->rows = [];
            $keys = $this->parent_->getLocalKeys($this->parentKey);

            foreach ($cached as $row) {
                if (in_array($row->__get($this->key), $keys)) {
                    $this->rows[] = $row;
                }
            }
        }

        return $this;
    }

    /**
     * Create a Row for this result's table
     * The row is bound to this result
     *
     * @param array $data Row data
     * @return Row
     */
    public function createRow(array $data = []) : Row
    {
        return $this->db->createRow($this->table, $data, $this);
    }

    /**
     * Get the database
     *
     * @return Database
     */
    public function getDatabase() : Database
    {
        return $this->db;
    }

    /**
     * Get the root result
     *
     * @return Result|Row
     */
    public function getRoot() : Result|Row
    {
        if (!$this->parent_) {
            return $this;
        }

        return $this->parent_->getRoot();
    }

    /**
     * Get the table of this result
     *
     * @return string
     */
    public function getTable() : string
    {
        return $this->table;
    }

    /**
     * Get $key values of this result
     *
     * @param string $key
     * @return array
     */
    public function getLocalKeys(string $key) : array
    {
        $this->execute();

        return $this->getKeys($this->rows, $key);
    }

    /**
     * Get global $key values of the result, i.e., disregarding its parent
     *
     * @param string $key
     * @return array
     */
    public function getGlobalKeys(string $key) : array
    {
        $this->execute();

        return $this->getKeys($this->globalRows, $key);
    }

    /**
     * Get $key values of given rows
     *
     * @param Row[] $rows
     * @param string $key
     * @return array
     * @throws \LogicExeption
     */
    protected function getKeys(array $rows, string $key) : array
    {
        if (count($rows) > 0 && !$rows[0]->hasProperty($key)) {
            throw new \LogicException('"' . $key . '" does not exist in "' . $this->table . '" result');
        }

        $keys = array();

        foreach ($rows as $row) {
            if ($row->__isset($key)) {
                $keys[] = $row->__get($key);
            }
        }

        return array_values(array_unique($keys));
    }

    /**
     * Get value from cache
     *
     * @param string $key
     * @return null|mixed
     */
    public function getCache(string $key) : mixed
    {
        return isset($this->_cache[$key]) ? $this->_cache[$key] : null;
    }

    /**
     * Set cache value
     *
     * @param string $key
     * @param mixed $value
     * @return $this;
     */
    public function setCache(string $key, mixed $value) : self
    {
        $this->_cache[$key] = $value;

        return $this;
    }

    /**
     * Is this result a single association, i.e. not a list of rows?
     *
     * @return bool|null
     */
    public function isSingle() : ?bool
    {
        return $this->single;
    }

    /**
     * Fetch the next row in this result
     *
     * @return Row|null
     */
    public function fetch() : ?Row
    {
        $this->execute();

        return isset($this->rows[0]) ? $this->rows[0] : null;
    }

    /**
     * Fetch all rows in this result
     *
     * @return Row[]
     */
    public function fetchAll() : array
    {
        $this->execute();

        return $this->rows;
    }

    /**
     * Return number of rows in this result
     *
     * @return int
     */
    public function rowCount() : int
    {
        $this->execute();

        return count($this->rows);
    }

    // Manipulation

    /**
     * Insert one ore more rows into the table of this result
     * See Database::insert for information on $method
     *
     * @param array $rows
     * @param string|null $method
     * @return null|\PDOStatement
     */
    public function insert(array $rows, ?string $method = null) : ?\PDOStatement
    {
        return $this->db->insert($this->table, $rows, $method);
    }

    /**
     * Update the rows matched by this result, setting $data
     *
     * @param array $data
     * @return null|\PDOStatement
     */
    public function update(array $data) : ?\PDOStatement
    {
        // if this is an association result or it is limited,
        // create specific result for local rows and execute

        if ($this->parent_ || isset($this->limitCount)) {
            return $this->primaryResult()->update($data);
        }

        return $this->db->update($this->table, $data, $this->where, $this->whereParams);
    }

    /**
     * Delete all rows matched by this result
     *
     * @return \PDOStatement
     */
    public function delete() : \PDOStatement
    {
        // if this is an association result or it is limited,
        // create specific result for local rows and execute

        if ($this->parent_ || isset($this->limitCount)) {
            return $this->primaryResult()->delete();
        }

        return $this->db->delete($this->table, $this->where, $this->whereParams);
    }

    /**
     * Return a new basic result which selects all rows in this result by primary key
     *
     * @return Result
     */
    public function primaryResult() : Result
    {
        $result = $this->db->table($this->table);
        $primary = $this->db->getPrimary($this->table);

        if (is_array($primary)) {
            $this->execute();
            $or = array();

            foreach ($this->rows as $row) {
                $and = array();

                foreach ($primary as $column) {
                    $and[] = $this->db->is($column, $row->__get($column));
                }

                $or[] = "( " . implode(" AND ", $and) . " )";
            }

            return $result->where(implode(" OR ", $or));
        }

        return $result->where($primary, $this->getLocalKeys($primary));
    }

    // Select

    /**
     * Return a new result with an additional expression to the SELECT part
     *
     * @param string $expr
     * @return Result
     */
    public function select(string $expr) : Result
    {
        $clone = clone $this;

        if ($clone->select === null) {
            $clone->select = func_get_args();
        } else {
            $clone->select = array_merge($clone->select, func_get_args());
        }

        return $clone;
    }

    public function expr(string $expr) : Result
    {
        return $this->select($expr);
    }

    /**
     * Add a WHERE condition (multiple are combined with AND)
     *
     * @param string|array $condition
     * @param string|array|null $params
     * @return Result
     */
    public function where(string|array $condition, string|array|null $params = []) : Result
    {
        $clone = clone $this;

        // conditions in key-value array
        if (is_array($condition)) {
            foreach ($condition as $c => $params) {
                $clone = $clone->where($c, $params);
            }

            return $clone;
        }

        // shortcut for basic "column is (in) value"
        if (preg_match('/^[a-z0-9_.`"]+$/i', $condition)) {
            $clone->where[] = $clone->db->is($condition, $params);

            return $clone;
        }

        if (!is_array($params)) {
            $params = func_get_args();
            array_shift($params);
        }

        $clone->where[] = $condition;
        $clone->whereParams = array_merge($clone->whereParams, $params);

        return $clone;
    }

    /**
     * Add an OR WHERE condition (multiple are combined with OR)
     *
     * @param string|array $condition
     * @param string|array|null $params
     * @return Result
     */
    public function orWhere(string|array $condition, string|array|null $params = []) : Result
    {
        $clone = clone $this;

        // conditions in key-value array
        if (is_array($condition)) {
            foreach ($condition as $c => $params) {
                $clone = $clone->orWhere($c, $params);
            }

            return $clone;
        }

        // shortcut for basic "column is (in) value"
        if (preg_match('/^[a-z0-9_.`"]+$/i', $condition)) {
            $clone->orWhere[] = $clone->db->is($condition, $params);

            return $clone;
        }

        if (!is_array($params)) {
            $params = func_get_args();
            array_shift($params);
        }

        $clone->orWhere[] = $condition;
        $clone->whereParams = array_merge($clone->whereParams, $params);

        return $clone;
    }


    /**
     * Add a "$column is not $value" condition to WHERE (multiple are combined with AND)
     *
     * @param string|array $column
     * @param string|array|null $value
     * @return Result
     */
    public function whereNot(string|array $column, string|array|null $value = null) : Result
    {
        $clone = clone $this;

        // conditions in key-value array
        if (is_array($column)) {
            foreach ($column as $c => $params) {
                $clone = $clone->whereNot($c, $params);
            }

            return $clone;
        }

        $clone->where[] = $this->db->isNot($column, $value);

        return $clone;
    }

    /**
     * Add a "or $column is not $value" condition to WHERE (multiple are combined with OR)
     *
     * @param string|array $column
     * @param string|array|null $value
     * @return Result
     */
    public function orWhereNot(string|array $column, string|array|null $value = null) : Result
    {
        $clone = clone $this;

        // conditions in key-value array
        if (is_array($column)) {
            foreach ($column as $c => $params) {
                $clone = $clone->orWhereNot($c, $params);
            }

            return $clone;
        }

        $clone->orWhere[] = $this->db->isNot($column, $value);

        return $clone;
    }


    /**
     * Add an GROUP BY column
     *
     * @param string $column
     * @return Result
     */
    public function groupBy(string $column) : Result
    {
        $clone = clone $this;

        $clone->groupBy[] = $this->db->quoteIdentifier($column);

        return $clone;
    }

    /**
     * Add an HAVING expression
     *
     * @param string $having
     * @return Result
     */
    public function having(string $having) : Result
    {
        $clone = clone $this;

        $clone->having[] = $having;

        return $clone;
    }

    /**
     * Add an ORDER BY column and direction
     *
     * @param string $column
     * @param string $direction
     * @return Result
     */
    public function orderBy(string $column, string $direction = "ASC", string $position = 'end') : Result
    {
        $clone = clone $this;

        switch($position) {
            case 'start':
                if ($direction === true) {
                    array_unshift($clone->orderBy, $column);
                } else {
                    array_unshift($clone->orderBy, $this->db->quoteIdentifier($column) . " " . $direction);
                }
        
                break;
            case 'end':
            default:
                if ($direction === true) {
                    $clone->orderBy[] = $column;
                } else {
                    $clone->orderBy[] = $this->db->quoteIdentifier($column) . " " . $direction;
                }
    
                break;
        }


        return $clone;
    }

    /**
     * Set a result limit and optionally an offset
     *
     * @param int $count
     * @param int|null $offset
     * @return Result
     * @throws \LogicException
     */
    public function limit(int $count, ?int $offset = null) : Result
    {
        if ($this->parent_) {
            throw new \LogicException('Cannot limit referenced result');
        }

        $clone = clone $this;

        $clone->limitCount = $count;
        $clone->limitOffset = $offset;

        return $clone;
    }

    /**
     * Set a paged limit
     * Pages start at 1
     *
     * @param int $pageSize
     * @param int $page
     * @return Result
     * @throws \LogicException
     */
    public function paged(int $pageSize, int $page) : Result
    {
        if ($page < 1) {
            throw new \LogicException('Page parameters starts at 1');
        }
        return $this->limit($pageSize, ($page - 1) * $pageSize);
    }

    // Aggregate functions

    /**
     * Count number of rows
     * Implements Countable
     *
     * @param string $expr
     * @return int
     */
    public function count(string $expr = "*") : int
    {
        return (int) $this->aggregate("COUNT(" . $expr . ")");
    }

    /**
     * resets specific parts
     * 
     * @param string $partName
     * @return Result
     */
    public function removePart($partName) : Result
    {
        $clone = clone $this;

        switch ($partName) {
            case 'where': $clone->where = array(); break;
            case 'groupBy': $clone->groupBy = array(); break;
            case 'having': $clone->having = array(); break;
            case 'orderBy': $clone->orderBy = array(); break;
            case 'limitCount': $clone->limitCount = null; break;
            case 'limitOffset': $clone->limitOffset = null; break;
            case 'params': $clone->whereParams = array(); break;     
        }

        return $clone;
    }

    /**
     * Return minimum value from an expression
     *
     * @param string $expr
     * @return mixed
     * @throws \LogicException
     */
    public function min(string $expr) : mixed
    {
        return $this->aggregate("MIN(" . $expr . ")");
    }

    /**
     * Return maximum value from an expression
     *
     * @param string $expr
     * @return mixed
     * @throws \LogicException
     */
    public function max(string $expr) : mixed
    {
        return $this->aggregate("MAX(" . $expr . ")");
    }

    /**
     * Return sum of values in an expression
     *
     * @param string $expr
     * @return mixed
     * @throws \LogicException
     */
    public function sum(string $expr) : mixed
    {
        return $this->aggregate("SUM(" . $expr . ")");
    }

    /**
     * Execute aggregate function and return value
     *
     * @param string $function
     * @return mixed
     * @throws \LogicException
     */
    public function aggregate(string $function)
    {
        if ($this->parent_) {
            throw new \LogicException('Cannot aggregate referenced result');
        }

        $statement = $this->db->select($this->table, [
            'expr' => $function,
            'where' => $this->where,
            'orWhere' => $this->orWhere,
            'groupBy' => $this->groupBy,
            'having' => $this->having,
            'orderBy' => $this->orderBy,
            'limitCount' => $this->limitCount,
            'limitOffset' => $this->limitOffset,
            'params' => $this->whereParams,
        ]);

        foreach ($statement->fetch() as $return) {
            return $return;
        }

        return null;
    }

    /**
     * IteratorAggregate
     *
     * @return \ArrayIterator
     */
    public function getIterator() : \ArrayIterator
    {
        $this->execute();

        return new \ArrayIterator($this->rows);
    }

    /**
     * Get a JSON string defining the SELECT information of this Result
     * Used as identification in caches
     *
     * @return string|false
     */
    public function getDefinition() : string|false
    {
        return json_encode([
            'table' => $this->table,
            'select' => $this->select,
            'where' => $this->where,
            'orWhere' => $this->orWhere,
            'whereParams' => $this->whereParams,
            'orderBy' => $this->orderBy,
            'limitCount' => $this->limitCount,
            'limitOffset' => $this->limitOffset,
        ]);
    }

    /**
     * Get parent result or row, if any
     *
     * @return Result|Row
     */
    public function getParent() : Result|Row
    {
        return $this->parent_;
    }

    /**
     * clone
     */
    public function __clone()
    {
        $this->rows = null;
        $this->globalRows = null;
    }

    /**
     * Implements JsonSerialize
     *
     * @return Row[]
     */
    public function jsonSerialize() : array
    {
        return $this->fetchAll();
    }
}
