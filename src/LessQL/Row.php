<?php

namespace LessQL;

/**
 * Represents a row of an SQL table (associative)
 */
class Row implements \ArrayAccess, \IteratorAggregate, \JsonSerializable
{
    /** @var string */
    protected string $_table;

    /** @var Result|null */
    protected ?Result $_result;

    /** @var array */
    protected array $_properties = [];

    /** @var array */
    protected array $_modified = [];

    /** @var null|string|array */
    protected string|array|null $_originalId;

    /** @var array */
    protected array $_cache = [];

    /**
     * Constructor
     * Use $db->createRow() instead
     *
     * @param Database $db
     * @param string $name
     * @param array $properties
     * @param Result|null $result
     */
    public function __construct(protected Database $db, $name, array $properties = [], ?Result $result = null)
    {
        $this->_result = $result;
        $this->_table = $this->db->getAlias($name);

        $this->setData($properties);
    }

    /**
     * Get a property
     *
     * @param string $column
     * @return mixed
     */
    public function &__get(string $column)
    {
        if (!isset($this->_properties[$column])) {
            // is returning by reference
            $null = null;
            return $null;
        }

        return $this->_properties[$column];
    }

    /**
     * Set a property
     *
     * @param string $column
     * @param mixed $value
     * @return $this
     */
    public function __set(string $column, mixed $value) : self
    {
        if (isset($this->_properties[$column]) && $this->_properties[$column] === $value) {
            return $this;
        }

        // convert arrays to Rows or list of Rows

        if (is_array($value)) {
            $name = preg_replace('/List$/', '', $column);
            $table = $this->getDatabase()->getAlias($name);

            if ($name === $column) { // row
                $value = $this->getDatabase()->createRow($table, $value);
            } else { // list
                foreach ($value as $i => $v) {
                    $value[$i] = $this->getDatabase()->createRow($table, $v);
                }
            }
        }

        $this->_properties[$column] = $value;
        $this->_modified[$column] = $value;

        return $this;
    }

    /**
     * Check if property is not null
     *
     * @param string $column
     * @return bool
     */
    public function __isset(string $column) : bool
    {
        return isset($this->_properties[$column]);
    }

    /**
     * Remove a property from this row
     * Property will be ignored when saved, different to setting to null
     *
     * @param string $column
     * @return void
     */
    public function __unset(string $column) : void
    {
        unset($this->_properties[$column]);
        unset($this->_modified[$column]);
    }

    /**
     * Get referenced row(s) by name. Suffix "List" gets many rows using
     * a back reference.
     *
     * @param string $name
     * @param array|null $args
     * @return mixed
     */
    public function __call(string $name, ?array $args) : mixed
    {
        array_unshift($args, $name);

        return call_user_func_array(array($this, 'referenced'), $args);
    }

    /**
     * Get referenced row(s) by name. Suffix "List" gets many rows using
     * a back reference.
     *
     * @param string $name
     * @param string|array|null $where
     * @param array $params
     * @return Result
     */
    public function referenced(string $name, string|array|null $where = null, array $params = []) : Result
    {
        $result = $this->getDatabase()->createResult($this, $name);

        if ($where !== null) {
            if (!is_array($params)) {
                $params = array_slice(func_get_args(), 2);
            }
            $result = $result->where($where, $params);
        }

        return $result;
    }

    /**
     * Get the row's id
     *
     * @return string|array|null
     */
    public function getId() : string|array|null
    {
        $primary = $this->getDatabase()->getPrimary($this->getTable());

        if (is_array($primary)) {
            $id = [];

            foreach ($primary as $column) {
                if (!isset($this[$column])) {
                    return null;
                }

                $id[$column] = $this[$column];
            }

            return $id;
        }

        return $this[$primary];
    }

    /**
     * Get row data
     *
     * @return array
     */
    public function getData() : array
    {
        $data = [];

        foreach ($this->_properties as $column => $value) {
            if ($value instanceof Row || is_array($value)) {
                continue;
            }

            $data[$column] = $value;
        }

        return $data;
    }

    /**
     * Set row data (extends the row)
     *
     * @param array $data
     * @return $this
     */
    public function setData(array $data) : self
    {
        foreach ($data as $column => $value) {
            $this->__set($column, $value);
        }

        return $this;
    }

    /**
     * Get the original id
     *
     * @return string|array|null
     */
    public function getOriginalId() : string|array|null
    {
        return $this->_originalId;
    }

    /**
     * Get modified data
     *
     * @return array
     */
    public function getModified() : array
    {
        $modified = [];

        foreach ($this->_modified as $column => $value) {
            if ($value instanceof Row || is_array($value)) {
                continue;
            }

            $modified[$column] = $value;
        }

        return $modified;
    }

    /**
     * Save this row
     * Also saves nested rows if $recursive is true (default)
     *
     * @param bool $recursive
     * @return $this
     * @throws \LogicException
     */
    public function save(bool $recursive = true) : self
    {
        $db = $this->getDatabase();
        $table = $this->getTable();

        if (!$recursive) { // just save the row
            $this->updateReferences();

            if (!$this->isClean()) {
                $primary = $db->getPrimary($table);

                if ($this->exists()) {
                    $idCondition = $this->getOriginalId();

                    if (!is_array($idCondition)) {
                        $idCondition = array($primary => $idCondition);
                    }

                    $db->table($table)
                        ->where($idCondition)
                        ->update($this->getModified());

                    $this->setClean();
                } else {
                    $db->table($table)
                        ->insert($this->getData());

                    if (!is_array($primary) && !isset($this[$primary])) {
                        $id = $db->lastInsertId($db->getSequence($table));

                        if (isset($id)) {
                            $this[$primary] = $id;
                        }
                    }

                    $this->setClean();
                }
            }

            return $this;
        }

        // make list of all rows in this tree

        $list = [];
        $this->listRows($list);
        $count = count($list);

        // keep iterating and saving until all references are known

        while (true) {
            $solvable = false;
            $clean = 0;

            foreach ($list as $row) {
                $row->updateReferences();

                $missing = $row->getMissing();

                if (empty($missing)) {
                    $row->save(false);
                    $row->updateBackReferences();
                    $solvable = true;
                }

                if ($row->isClean()) {
                    ++$clean;
                }
            }

            if (!$solvable) {
                throw new \LogicException(
                    'Cannot recursively save structure (' . $table . ') - add required values or allow NULL'
                );
            }

            if ($clean === $count) {
                break;
            }
        }

        return $this;
    }

    /**
     * @param array $list
     */
    protected function listRows(array &$list) : void
    {
        $list[] = $this;

        foreach ($this->_properties as $column => $value) {
            if ($value instanceof Row) {
                $value->listRows($list);
            } elseif (is_array($value)) {
                foreach ($value as $row) {
                    $row->listRows($list);
                }
            }
        }
    }

    /**
     * Check references and set respective keys
     * Returns list of keys to unknown references
     *
     * @return array
     */
    public function updateReferences() : array
    {
        $unknown = [];
        $db = $this->getDatabase();

        foreach ($this->_properties as $column => $value) {
            if ($value instanceof Row) {
                $key = $db->getReference($this->getTable(), $column);
                $this[$key] = $value->getId();
            }
        }

        return $unknown;
    }

    /**
     * Check back references and set respective keys
     *
     * @return $this
     */
    public function updateBackReferences() : self
    {
        $id = $this->getId();

        if (is_array($id)) {
            return $this;
        }

        $db = $this->getDatabase();

        foreach ($this->_properties as $column => $value) {
            if (is_array($value)) {
                $key = $db->getBackReference($this->getTable(), $column);

                foreach ($value as $row) {
                    $row->{ $key } = $id;
                }
            }
        }

        return $this;
    }

    /**
     * Get missing columns, i.e. any that is null but required by the schema
     *
     * @return array
     */
    public function getMissing() : array
    {
        $missing = [];
        $required = $this->getDatabase()->getRequired($this->getTable());

        foreach ($required as $column => $true) {
            if (!isset($this[$column])) {
                $missing[] = $column;
            }
        }

        return $missing;
    }

    /**
     * Update this row directly
     *
     * @param $data
     * @param bool $recursive
     * @return $this
     */
    public function update($data, $recursive = true) : Row
    {
        return $this->setData($data)->save($recursive);
    }

    /**
     * Delete this row
     *
     * @return $this|Row
     */
    public function delete() : Row
    {
        $db = $this->getDatabase();
        $table = $this->getTable();

        $result = $db->table($table);

        $idCondition = $this->getOriginalId();

        if ($idCondition === null) {
            return $this;
        }

        if (!is_array($idCondition)) {
            $primary = $db->getPrimary($table);
            $idCondition = array($primary => $idCondition);
        }

        $result->where($idCondition)->delete();

        $this->_originalId = null;

        return $this->setDirty();
    }

    /**
     * Does this row exist?
     *
     * @return bool
     */
    public function exists() : bool
    {
        return $this->_originalId !== null;
    }

    /**
     * Is this row clean, i.e. in sync with the database?
     *
     * @return bool
     */
    public function isClean() : bool
    {
        return empty($this->_modified);
    }

    /**
     * Set this row to "clean" state, i.e. in sync with database
     *
     * @return $this
     * @throws \LogicException
     */
    public function setClean() : self
    {
        $id = $this->getId();

        if ($id === null) {
            throw new \LogicException('Cannot set Row "clean" without id');
        }

        $this->_originalId = $id;
        $this->_modified = [];

        return $this;
    }

    /**
     * Set this row to "dirty" state, i.e. out of sync with database
     *
     * @return $this
     */
    public function setDirty() : self
    {
        $this->_modified = $this->_properties; // copy...

        return $this;
    }

    /**
     * Get root result or row
     *
     * @return Result|Row
     */
    public function getRoot() : Result|Row
    {
        $result = $this->getResult();

        if ($result) {
            return $result->getRoot();
        }

        return $this;
    }

    /**
     * Get value from cache
     *
     * @param $key
     * @return mixed
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
     */
    public function setCache(string $key, mixed $value) : void
    {
        $this->_cache[$key] = $value;
    }

    /**
     * Get column, used by result if row is parent
     *
     * @param string $key
     * @return array
     */
    public function getLocalKeys(string $key) : array
    {
        if (isset($this[$key])) {
            return [ $this[$key] ];
        }

        return [];
    }

    /**
     * Get global keys of parent result, or column if row is root
     *
     * @param string $key
     * @return array
     */
    public function getGlobalKeys(string $key) : array
    {
        $result = $this->getResult();

        if ($result) {
            return $result->getGlobalKeys($key);
        }

        return $this->getLocalKeys($key);
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
     * Get the bound result, if any
     *
     * @return Result|null
     */
    public function getResult() : ?Result
    {
        return $this->_result;
    }

    /**
     * Get the table
     *
     * @return string
     */
    public function getTable() : string
    {
        return $this->_table;
    }

    /**
     * Returns true if the given property exists, even if its value is null
     *
     * @param string $name Property name to check
     * @return bool
     */
    public function hasProperty($name) : bool
    {
        return array_key_exists($name, $this->_properties);
    }

    // ArrayAccess

    /**
     * @param string $offset
     * @return bool
     */
    public function offsetExists($offset) : bool
    {
        return $this->__isset($offset);
    }

    /**
     * @param string $offset
     * @return mixed
     */
    public function &offsetGet($offset) : mixed
    {
        return $this->__get($offset);
    }

    /**
     * @param string $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value) : void
    {
        $this->__set($offset, $value);
    }

    /**
     * @param string $offset
     */
    public function offsetUnset($offset) : void
    {
        $this->__unset($offset);
    }

    // IteratorAggregate

    /**
     * @return \ArrayIterator
     */
    public function getIterator() : \ArrayIterator
    {
        return new \ArrayIterator($this->_properties);
    }

    // JsonSerializable

    /**
     * @return array
     */
    public function jsonSerialize() : array
    {
        $array = [];

        foreach ($this->_properties as $key => $value) {
            if ($value instanceof \JsonSerializable) {
                $array[$key] = $value->jsonSerialize();
            } elseif ($value instanceof \DateTime) {
                $array[$key] = $value->format('Y-m-d H:i:s');
            } elseif (is_array($value)) { // list of Rows

                foreach ($value as $i => $row) {
                    $value[$i] = $row->jsonSerialize();
                }

                $array[$key] = $value;
            } else {
                $array[$key] = $value;
            }
        }

        return $array;
    }
}
