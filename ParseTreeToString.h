/**
 * @file ParseTreeToString.h - SQL unparsing class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include <string>
#include <vector>
#include "SQLParser.h"

/**
 * @class ParseTreeToString - class for unparsing a Hyrise Abstract Syntax Tree
 */
class ParseTreeToString {
public:
    /**
     * Unparse a Hyrise AST into an SQL statement.
     * @param statement  Hyrise AST pointer
     * @returns          string of the SQL statement equivalent to what was parsed
     */
    static std::string statement(const hsql::SQLStatement *statement);

    /**
     * Check if a given word is a reserved word in our version of SQL.
     */
    static bool is_reserved_word(std::string word);

private:
    // reserved words
    static const std::vector<std::string> reserved_words;

    // sub-expressions
    static std::string operator_expression(const hsql::Expr *expr);

    static std::string expression(const hsql::Expr *expr);

    static std::string table_ref(const hsql::TableRef *table);

    static std::string column_definition(const hsql::ColumnDefinition *col);

    static std::string select(const hsql::SelectStatement *stmt);

    static std::string insert(const hsql::InsertStatement *stmt);

    static std::string create(const hsql::CreateStatement *stmt);

    static std::string drop(const hsql::DropStatement *stmt);

    static std::string show(const hsql::ShowStatement *stmt);
};

