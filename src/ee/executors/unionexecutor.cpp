/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"

#include "unionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/unionnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

namespace voltdb {

namespace detail {

struct SetOperator {
    typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
        TupleMap;

    SetOperator(UnionPlanNode* node, bool is_all);

    virtual ~SetOperator() {
    }

    bool processTuples() {
        // Clear the temp table
        m_temp_table->deleteAllTuples(false);
        // Clear tuples.
        m_tuples.clear();

        return processTuplesDo();
    }

    TableTuple pullNextTuple();

    static SetOperator* getSetOperator(UnionPlanNode* node);

    protected:
        virtual bool processTuplesDo() = 0;

        std::vector<AbstractExecutor*> m_children;
        const TupleSchema* m_schema;
        // An intermediate table to accumulate tuples
        boost::scoped_ptr<TempTable> m_temp_table;
        bool m_is_all;
        // Map to keep candidate tuples. The key is the tuple itself
        // The value - tuple's repeat count in the final table.
        TupleMap m_tuples;

        // Iterator and counter to keep state between the next_pull calls
        TupleMap::const_iterator m_output_iterator;
        size_t m_count;
};

SetOperator::SetOperator(UnionPlanNode* node, bool is_all) :
        m_children(),
        m_schema(node->getOutputTable()->schema()),
        m_temp_table(),
        m_is_all(is_all),
        m_output_iterator(),
        m_count(0) {

    std::vector<AbstractPlanNode*>& children = node->getChildren();
    for (size_t cnt = 0; cnt < children.size(); cnt++) {
        m_children.push_back(children[cnt]->getExecutor());
    }
    TempTable* temp_output = dynamic_cast<TempTable*>(node->getOutputTable());
    assert(temp_output != NULL);
    m_temp_table.reset(TableFactory::getCopiedTempTable(
        temp_output->databaseId(), temp_output->name(), temp_output,
        temp_output->m_limits));
}

TableTuple SetOperator::pullNextTuple() {
    if (m_output_iterator != m_tuples.end()) {
        if (m_count++ < m_output_iterator->second) {
            return m_output_iterator->first;
        } else {
            m_count = 0;
            if (++m_output_iterator != m_tuples.end()) {
                ++m_count;
                return m_output_iterator->first;
            } else {
                return TableTuple(m_schema);
            }
        }
    } else {
        return TableTuple(m_schema);
    }
}

struct UnionSetOperator : public SetOperator {
    UnionSetOperator(UnionPlanNode* node, bool is_all) :
       SetOperator(node, is_all)
       {}

    protected:
        bool processTuplesDo();

    private:
        void insertTuple(TableTuple& tuple);

};

bool UnionSetOperator::processTuplesDo() {
    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table. Only distinct tuples are retained.
    //
    for (size_t cnt = 0; cnt < m_children.size(); cnt++) {
        AbstractExecutor* childExec = m_children[cnt];
        assert(childExec);
        TableTuple tuple(m_schema);
        for (tuple = childExec->next_pull();
               tuple.isNullTuple() == false;
               tuple = childExec->next_pull())  {
             insertTuple(tuple);
        }
    }
    // reset the iterator
    m_output_iterator = m_tuples.begin();
    return true;
}

inline
void UnionSetOperator::insertTuple(TableTuple& tuple) {
    TupleMap::iterator mapIt = m_tuples.find(tuple);
    if (mapIt == m_tuples.end()) {
        TableTuple inserted_tuple = m_temp_table->insertTupleNonVirtual(tuple);
        m_tuples.insert(std::make_pair(inserted_tuple, 1));
    } else if (m_is_all) {
        mapIt->second++;
    }
}

struct ExceptIntersectSetOperator : public SetOperator {
    ExceptIntersectSetOperator(UnionPlanNode* node,
        bool is_all, bool is_except);

    protected:
        bool processTuplesDo();

    private:
        void collectTuples(size_t idx, TupleMap& tuple_map);
        void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
        void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);

        bool m_is_except;
};

ExceptIntersectSetOperator::ExceptIntersectSetOperator(
    UnionPlanNode* node, bool is_all, bool is_except) :
        SetOperator(node, is_all), m_is_except(is_except) {
}

bool ExceptIntersectSetOperator::processTuplesDo() {
    // Collect all tuples from the first set
    collectTuples(0, m_tuples);

    //
    // For each remaining input table, collect its tuple into a separate map
    // and substract/intersect it from/with the first one
    //
    TupleMap next_tuples;
    for (size_t cnt = 1; cnt < m_children.size(); cnt++) {
        next_tuples.clear();
        collectTuples(cnt, next_tuples);
        if (m_is_except) {
            exceptTupleMaps(m_tuples, next_tuples);
        } else {
            intersectTupleMaps(m_tuples, next_tuples);
        }
    }
    // reset the iterator
    m_output_iterator = m_tuples.begin();
    return true;
}

void ExceptIntersectSetOperator::collectTuples(size_t idx, TupleMap& tuple_map) {
    AbstractExecutor* childExec = m_children[idx];
    assert(childExec);
    TableTuple tuple(m_schema);

    for (tuple = childExec->next_pull();
           tuple.isNullTuple() == false;
           tuple = childExec->next_pull())  {
        TupleMap::iterator mapIt = tuple_map.find(tuple);
        if (mapIt == tuple_map.end()) {
            TableTuple inserted_tuple = m_temp_table->insertTupleNonVirtual(tuple);
            tuple_map.insert(std::make_pair(inserted_tuple, 1));
        } else if (m_is_all) {
            ++mapIt->second;
        }
    }
}

void ExceptIntersectSetOperator::exceptTupleMaps(TupleMap& map_a, TupleMap& map_b) {
    const static size_t zero_val(0);
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b != map_b.end()) {
            it_a->second = (it_a->second > it_b->second) ?
                std::max(it_a->second - it_b->second, zero_val) : zero_val;
            if (it_a->second == zero_val) {
                it_a = map_a.erase(it_a);
            } else {
                ++it_a;
            }
        } else {
            ++it_a;
        }
    }
}

void ExceptIntersectSetOperator::intersectTupleMaps(TupleMap& map_a, TupleMap& map_b) {
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b == map_b.end()) {
            it_a = map_a.erase(it_a);
        } else {
            it_a->second = std::min(it_a->second, it_b->second);
            ++it_a;
        }
    }
}

SetOperator* SetOperator::getSetOperator(UnionPlanNode* node) {
    UnionType unionType = node->getUnionType();
    switch (unionType) {
        case UNION_TYPE_UNION_ALL:
            return new UnionSetOperator(node, true);
        case UNION_TYPE_UNION:
            return new UnionSetOperator(node, false);
        case UNION_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator(node, true, true);
        case UNION_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator(node, false, true);
        case UNION_TYPE_INTERSECT_ALL:
            return new ExceptIntersectSetOperator(node, true, false);
        case UNION_TYPE_INTERSECT:
            return new ExceptIntersectSetOperator(node, false, false);
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", unionType);
            return NULL;
    }
}

} // namespace detail

UnionExecutor::UnionExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) :
    AbstractExecutor(engine, abstract_node), m_setOperator()
{}

bool UnionExecutor::p_init(AbstractPlanNode* abstract_node,
                           TempTableLimits* limits)
{
    VOLT_TRACE("init Union Executor");

    UnionPlanNode* node = dynamic_cast<UnionPlanNode*>(abstract_node);
    assert(node);

    //
    // First check to make sure they have the same number of columns
    //
    assert(node->getInputTables().size() > 0);
    for (int table_ctr = 1, table_cnt = (int)node->getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
        if (node->getInputTables()[0]->columnCount() != node->getInputTables()[table_ctr]->columnCount()) {
            VOLT_ERROR("Table '%s' has %d columns, but table '%s' has %d"
                       " columns",
                       node->getInputTables()[0]->name().c_str(),
                       node->getInputTables()[0]->columnCount(),
                       node->getInputTables()[table_ctr]->name().c_str(),
                       node->getInputTables()[table_ctr]->columnCount());
            return false;
        }
    }

    //
    // Then check that they have the same types
    // The two loops here are broken out so that we don't have to keep grabbing the same column for input_table[0]
    //

    // get the first table
    const TupleSchema *table0Schema = node->getInputTables()[0]->schema();
    // iterate over all columns in the first table
    for (int col_ctr = 0, col_cnt = table0Schema->columnCount(); col_ctr < col_cnt; col_ctr++) {
        // get the type for the current column
        ValueType type0 = table0Schema->columnType(col_ctr);

        // iterate through all the other tables, comparing one column at a time
        for (int table_ctr = 1, table_cnt = (int)node->getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
            // get another table
            const TupleSchema *table1Schema = node->getInputTables()[table_ctr]->schema();
            ValueType type1 = table1Schema->columnType(col_ctr);
            if (type0 != type1) {
                // TODO: DEBUG
                VOLT_ERROR("Table '%s' has value type '%s' for column '%d',"
                           " table '%s' has value type '%s' for column '%d'",
                           node->getInputTables()[0]->name().c_str(),
                           getTypeName(type0).c_str(),
                           col_ctr,
                           node->getInputTables()[table_ctr]->name().c_str(),
                           getTypeName(type1).c_str(), col_ctr);
                return false;
            }
        }
    }
    //
    // Create our output table that will hold all the tuples that we are appending into.
    // Since we're are assuming that all of the tables have the same number of columns with
    // the same format. Therefore, we will just grab the first table in the list
    //
    node->setOutputTable(TableFactory::getCopiedTempTable(node->databaseId(),
                                                          node->getInputTables()[0]->name(),
                                                          node->getInputTables()[0],
                                                          limits));

    m_setOperator.reset(detail::SetOperator::getSetOperator(node));
    return true;
}

bool UnionExecutor::p_execute(const NValueArray &params) {
    return m_setOperator->processTuples();
}

bool UnionExecutor::support_pull() const
{
    return true;
}

void UnionExecutor::p_pre_execute_pull(const NValueArray &params)
{
    m_setOperator->processTuples();
}

TableTuple UnionExecutor::p_next_pull()
{
    return m_setOperator->pullNextTuple();
}

}
