<?php

namespace LessQL;

/**
 * SQL Literal
 */
class Literal
{
    /**
     * Constructor
     *
     * @param string
     */
    public function __construct(public string $value){ }

    /**
     * Return the literal value
     *
     * @return string
     */
    public function __toString()
    {
        return $this->value;
    }
}
