// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/cow.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>

#include "common/status.h"
namespace starrocks {

class CowTest : public testing::Test {
public:
    class IColumn : public Cow<IColumn> {
    private:
        friend class Cow<IColumn>;

    public:
        IColumn() { std::cout << "IColumn constructor" << std::endl; }
        IColumn(const IColumn&) { std::cout << "IColumn copy constructor" << std::endl; }
        virtual ~IColumn() = default;

        virtual MutablePtr clone() const = 0;
        virtual int get() const { return -1; };
        virtual void set(int value){};
        virtual int get(int i) const { return -1; }
        virtual void set(int i, int val) {}

        /// If the column contains subcolumns (such as Array, Nullable, etc), do callback on them.
        /// Shallow: doesn't do recursive calls; don't do call for itself.
        using ColumnCallback = std::function<void(Ptr&)>;
        virtual void for_each_subcolumn(ColumnCallback) {}

        MutablePtr mutate() const&& {
            MutablePtr res = try_mutate();
            res->for_each_subcolumn([](Ptr& subcolumn) { subcolumn = std::move(*subcolumn).mutate(); });
            return res;
        }

        [[nodiscard]] static MutablePtr mutate(Ptr ptr) {
            MutablePtr res = ptr->try_mutate(); /// Now use_count is 2.
            ptr.reset();                        /// Reset use_count to 1.
            res->for_each_subcolumn([](Ptr& subcolumn) { subcolumn = IColumn::mutate(std::move(subcolumn)); });
            return res;
        }
    };

    using ColumnPtr = IColumn::Ptr;
    using MutableColumnPtr = IColumn::MutablePtr;
    using Columns = std::vector<ColumnPtr>;
    using MutableColumns = std::vector<MutableColumnPtr>;

    template <typename Base, typename Derived, typename AncestorBase = Base>
    class ColumnFactory : public Base {
    private:
        Derived* mutable_derived() { return static_cast<Derived*>(this); }
        const Derived* derived() const { return static_cast<const Derived*>(this); }

    public:
        template <typename... Args>
        ColumnFactory(Args&&... args) : Base(std::forward<Args>(args)...) {}

        using AncestorBaseType = std::enable_if_t<std::is_base_of_v<AncestorBase, Base>, AncestorBase>;
    };

    // use ColumnFactory to create ConcreteColumn
    class ConcreteColumn final : public CowFactory<ColumnFactory<IColumn, ConcreteColumn>, ConcreteColumn> {
    public:
        int get() const override { return _data; }
        void set(int value) override { _data = value; }

        // not override
        void set_value(int val) { _data = val; }
        int get_value() { return _data; }

    private:
        friend class CowFactory<ColumnFactory<IColumn, ConcreteColumn>, ConcreteColumn>;

        explicit ConcreteColumn(int data) : _data(data) {
            std::cout << "ConcreteColumn constructor:" << _data << std::endl;
        }
        ConcreteColumn(const ConcreteColumn& col) {
            std::cout << "ConcreteColumn copy constructor" << std::endl;
            this->_data = col._data;
        }

    private:
        int _data;
    };
    using ConcreteColumnPtr = ConcreteColumn::Ptr;
    using ConcreteColumnMutablePtr = ConcreteColumn::MutablePtr;

    // ConcreteVectorColumn is a column with vector<int> data
    class ConcreteVectorColumn final
            : public CowFactory<ColumnFactory<IColumn, ConcreteVectorColumn>, ConcreteVectorColumn> {
    public:
        void set(int i, int val) override {
            DCHECK_LT(i, _data.size());
            _data[i] = val;
        }
        int get(int i) const override {
            DCHECK_LT(i, _data.size());
            return _data[i];
        }

    private:
        friend class CowFactory<ColumnFactory<IColumn, ConcreteVectorColumn>, ConcreteVectorColumn>;

        explicit ConcreteVectorColumn(std::vector<int> data) : _data(std::move(data)) {
            std::cout << "ConcreteVectorColumn constructor:" << std::endl;
        }
        ConcreteVectorColumn(const ConcreteVectorColumn& col) {
            std::cout << "ConcreteVectorColumn copy constructor" << std::endl;
            this->_data = col._data;
        }

    private:
        std::vector<int> _data;
    };
    using ConcreteVectorColumnPtr = ConcreteVectorColumn::Ptr;
    using ConcreteVectorColumnMutablePtr = ConcreteVectorColumn::MutablePtr;

    template <typename T>
    class MFixedLengthColumnBase
            : public CowFactory<ColumnFactory<IColumn, MFixedLengthColumnBase<T>>, MFixedLengthColumnBase<T>> {
        friend class CowFactory<ColumnFactory<IColumn, MFixedLengthColumnBase<T>>, MFixedLengthColumnBase<T>>;

    public:
        using ValueType = T;
    };

    template <typename T>
    class MFixedLengthColumn final : public CowFactory<ColumnFactory<MFixedLengthColumnBase<T>, MFixedLengthColumn<T>>,
                                                       MFixedLengthColumn<T>, IColumn> {
        friend class CowFactory<ColumnFactory<MFixedLengthColumnBase<T>, MFixedLengthColumn<T>>, MFixedLengthColumn<T>,
                                IColumn>;

        MFixedLengthColumn(const MFixedLengthColumn& col) {
            std::cout << "MFixedLengthColumn copy constructor" << std::endl;
        }

    public:
        using ValueType = T;
        using SuperClass = CowFactory<ColumnFactory<MFixedLengthColumnBase<T>, MFixedLengthColumn<T>>,
                                      MFixedLengthColumn<T>, IColumn>;
        MFixedLengthColumn() = default;
    };
    using MNullColumn = MFixedLengthColumn<uint8_t>;

    class ConcreteColumn2 final : public CowFactory<ColumnFactory<IColumn, ConcreteColumn2>, ConcreteColumn2> {
    private:
        friend class CowFactory<ColumnFactory<IColumn, ConcreteColumn2>, ConcreteColumn2>;

        ConcreteColumn2(MutableColumnPtr&& ptr) {
            std::cout << "ConcreteColumn2 constructor" << std::endl;
            _inner = ConcreteColumn::static_pointer_cast(std::move(ptr));
        }

        ConcreteColumn2(const ConcreteColumn2& col) {
            std::cout << "ConcreteColumn copy constructor" << std::endl;
            this->_inner = ConcreteColumn::static_pointer_cast(col._inner->clone());
            this->_null_column = MNullColumn::static_pointer_cast(col._null_column->clone());
        }

    public:
        int get() const override { return _inner->get(); }
        void set(int value) override { _inner->set(value); }

        void for_each_subcolumn(ColumnCallback callback) override {
            ColumnPtr inner;
            callback(inner);
            _inner = ConcreteColumn::static_pointer_cast(std::move(inner));

            // callback(_null_column);
            ColumnPtr null;
            callback(null);
            _null_column = MNullColumn::static_pointer_cast(std::move(null));
        }

    private:
        ConcreteColumnPtr _inner;
        MNullColumn::Ptr _null_column;
    };

    MutableColumnPtr move_func1(MutableColumnPtr&& col) { return std::move(col); }

    Columns move_func2(Columns&& cols) { return std::move(cols); }
    MutableColumns move_func2(MutableColumns&& cols) { return std::move(cols); }
};

TEST_F(CowTest, TestPtr) {
    {
        // mutable ptr can convert to immutable ptr
        ConcreteColumnPtr x = ConcreteColumn::create(1);
        EXPECT_EQ(1, x->get());
        x->set(2);
        EXPECT_EQ(2, x->get());
    }
    {
        auto x = ConcreteColumn::create(1);
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(1, x->use_count());
        ConcreteColumnPtr y = std::move(x);
        EXPECT_EQ(1, y->get());
        EXPECT_EQ(1, y->use_count());
        EXPECT_EQ(nullptr, x);
    }
    {
        ColumnPtr x = ConcreteColumn::create(1);
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(1, x->use_count());

        // share x
        ColumnPtr y = x;
        EXPECT_EQ(1, y->get());
        EXPECT_EQ(2, y->use_count());
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(2, x->use_count());
    }
}

TEST_F(CowTest, TestAssumeMutable) {
    MutableColumnPtr y;
    {
        auto x = ConcreteColumn::create(1);
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(1, x->use_count());

        // assume will add refcount
        y = x->as_mutable_ptr();
        EXPECT_EQ(1, y->get());
        EXPECT_EQ(2, y->use_count());
        EXPECT_EQ(2, x->use_count());
        // the address of x and y are the same
        EXPECT_EQ(x.get(), y.get());
        y->set(2);
        EXPECT_EQ(2, x->get());
        EXPECT_EQ(2, y->get());
    }
    EXPECT_EQ(2, y->get());
    EXPECT_EQ(1, y->use_count());
}

TEST_F(CowTest, TestColumnMoveFunc1) {
    MutableColumnPtr x = ConcreteColumn::create(1);
    EXPECT_EQ(1, x->get());

    MutableColumnPtr y = move_func1(std::move(x));
    EXPECT_EQ(1, y->get());
    EXPECT_EQ(nullptr, x);
}

TEST_F(CowTest, TestColumnsMove1) {
    MutableColumns v1;
    auto x = ConcreteColumn::create(1);
    v1.emplace_back(std::move(x));
    EXPECT_EQ(1, v1.size());
    EXPECT_EQ(1, v1[0]->get());
    EXPECT_EQ(nullptr, x);

    auto v2 = std::move(v1);
    EXPECT_EQ(1, v2.size());
    EXPECT_EQ(0, v1.size());
}

TEST_F(CowTest, TestColumnsMove2) {
    MutableColumns v1;
    for (int i = 0; i < 10; i++) {
        auto x = ConcreteColumn::create(1);
        v1.emplace_back(std::move(x));
    }
    EXPECT_EQ(10, v1.size());

    MutableColumns v2;
    for (auto& x : v1) {
        v2.emplace_back(std::move(x));
    }
    EXPECT_EQ(10, v2.size());
    EXPECT_EQ(10, v1.size());
    for (auto& x : v2) {
        EXPECT_EQ(1, x->get());
    }
    for (auto& x : v1) {
        EXPECT_EQ(nullptr, x);
    }
}

TEST_F(CowTest, TestColumnsMove3) {
    MutableColumns v1;
    for (int i = 0; i < 10; i++) {
        auto x = ConcreteColumn::create(1);
        v1.emplace_back(std::move(x));
    }
    EXPECT_EQ(10, v1.size());

    MutableColumns v2 = move_func2(std::move(v1));
    EXPECT_EQ(0, v1.size());
    EXPECT_EQ(10, v2.size());
    for (auto& x : v2) {
        EXPECT_EQ(1, x->get());
    }
}

TEST_F(CowTest, TestColumnPtrStaticPointerCast) {
    ColumnPtr x = ConcreteColumn::create(1);
    EXPECT_EQ(1, x->get());
    {
        ConcreteColumnPtr x1 = ConcreteColumn::create(x);
        // x1 is a deep copy of x, which its type is ConcreteColumn
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(1, x->get());
        // x1 and x are not shared
        EXPECT_NE(x1.get(), x.get());
        EXPECT_EQ(1, x1->use_count());
        EXPECT_EQ(1, x->use_count());
    }

    {
        // x1 is a shadow copy of x, which its type is ConcreteColumn
        ConcreteColumnPtr x1 = ConcreteColumn::static_pointer_cast(x);
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(2, x->get());
        EXPECT_EQ(x.get(), x1.get());
        EXPECT_EQ(2, x1->use_count());
        EXPECT_EQ(2, x->use_count());
    }
    {
        MutableColumnPtr x2 = x->as_mutable_ptr();
        x2->set(3);
        EXPECT_EQ(3, x2->get());
        EXPECT_EQ(3, x->get());
        EXPECT_EQ(x.get(), x2.get());
        EXPECT_EQ(2, x2->use_count());
        EXPECT_EQ(2, x->use_count());
    }
    EXPECT_EQ(3, x->get());
    EXPECT_EQ(1, x->use_count());

    // use std::move
    {
        ConcreteColumnPtr x1 = ConcreteColumn::static_pointer_cast(std::move(x));
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(nullptr, x);
        EXPECT_EQ(1, x1->use_count());
    }
}

TEST_F(CowTest, TestColumnPtrDynamicPointerCast) {
    ColumnPtr x = ConcreteColumn::create(1);
    EXPECT_EQ(1, x->get());
    {
        ConcreteColumnPtr x1 = ConcreteColumn::create(x);
        // x1 is a deep copy of x, which its type is ConcreteColumn
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(1, x->get());
        // x1 and x are not shared
        EXPECT_NE(x1.get(), x.get());
        EXPECT_EQ(1, x1->use_count());
        EXPECT_EQ(1, x->use_count());
    }

    {
        // x1 is a shadow copy of x, which its type is ConcreteColumn
        ConcreteColumnPtr x1 = ConcreteColumn::dynamic_pointer_cast(x);
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(2, x->get());
        EXPECT_EQ(x.get(), x1.get());
        EXPECT_EQ(2, x1->use_count());
        EXPECT_EQ(2, x->use_count());
    }
    {
        MutableColumnPtr x2 = x->as_mutable_ptr();
        x2->set(3);
        EXPECT_EQ(3, x2->get());
        EXPECT_EQ(3, x->get());
        EXPECT_EQ(x.get(), x2.get());
        EXPECT_EQ(2, x2->use_count());
        EXPECT_EQ(2, x->use_count());
    }
    EXPECT_EQ(3, x->get());
    EXPECT_EQ(1, x->use_count());

    // use std::move
    {
        ConcreteColumnPtr x1 = ConcreteColumn::dynamic_pointer_cast(std::move(x));
        x1->set(2);
        EXPECT_EQ(2, x1->get());
        EXPECT_EQ(nullptr, x);
        EXPECT_EQ(1, x1->use_count());
    }
}

TEST_F(CowTest, TestConcreteColumn2) {
    {
        ColumnPtr x = ConcreteColumn::create(1);
        ColumnPtr y = ConcreteColumn2::create(x->as_mutable_ptr());
    }
    { ColumnPtr y = ConcreteColumn2::create(ConcreteColumn::create(1)); }
    {
        auto x = ConcreteColumn::create(1);
        ColumnPtr y = ConcreteColumn2::create(x->as_mutable_ptr());
    }
    {
        auto x = ConcreteColumn::create(1);
        ColumnPtr y = ConcreteColumn2::create(std::move(x));
    }
    {
        MutableColumnPtr x = ConcreteColumn::create(1);
        ColumnPtr y = ConcreteColumn2::create(std::move(x));
    }
    {
        auto x = ConcreteColumn::create(1);
        ColumnPtr y = ConcreteColumn2::create(std::move(x));
    }
}

TEST_F(CowTest, TestClone) {
    ColumnPtr x = ConcreteColumn::create(1);

    EXPECT_EQ(1, x->get());
    auto cloned = x->clone();
    // cloned is a deep copy of x, which its type is IColum, is not ConcreteColumn
    cloned->set(2);
    EXPECT_EQ(1, x->get());
    EXPECT_EQ(2, cloned->get());

    (static_cast<ConcreteColumn*>(cloned.get()))->set_value(3);
    EXPECT_EQ(1, x->get());
    EXPECT_EQ(3, cloned->get());
}

TEST_F(CowTest, TestMutate) {
    ColumnPtr x = ConcreteColumn::create(1);

    {
        auto y1 = IColumn::mutate(x);
        y1->set(2);
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(2, y1->get());
        EXPECT_EQ(1, x->use_count());
        EXPECT_EQ(1, y1->use_count());
        EXPECT_NE(x.get(), y1.get());
    }

    {
        auto y2 = IColumn::mutate(std::move(x));
        EXPECT_EQ(1, y2->get());
        EXPECT_EQ(1, y2->use_count());
        EXPECT_EQ(nullptr, x);

        y2->set(3);
        EXPECT_EQ(3, y2->get());
    }
}

TEST_F(CowTest, TestCow) {
    using IColumnPtr = const IColumn*;
    IColumnPtr x_ptr;
    IColumnPtr y_ptr;
    ColumnPtr x = ConcreteColumn::create(1);
    ColumnPtr y = x;

    x_ptr = x.get();
    y_ptr = y.get();
    EXPECT_EQ(1, x->get());
    EXPECT_EQ(1, y->get());
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());
    EXPECT_EQ(x_ptr, y_ptr);
    {
        // move y to mut, y is moved
        // because x and y are shared which its use_cout is greater than 1, y is cloned(deep copy)
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        EXPECT_EQ(nullptr, y.get());
        mut->set(2);
        EXPECT_EQ(1, x->get());
        EXPECT_EQ(2, mut->get());
        EXPECT_EQ(1, x->use_count());
        EXPECT_EQ(1, mut->use_count());
        EXPECT_NE(x.get(), mut.get());

        y = std::move(mut);
        y_ptr = y.get();
        EXPECT_NE(x.get(), y.get());
        EXPECT_TRUE(x_ptr != y_ptr);
    }
    EXPECT_TRUE(x->get() == 1 && y->get() == 2);
    EXPECT_TRUE(x->use_count() == 1 && y->use_count() == 1);
    EXPECT_TRUE(x.get() != y.get());
    EXPECT_TRUE(x_ptr != y_ptr);
    EXPECT_TRUE(y_ptr == y.get());

    x = ConcreteColumn::create(0);
    EXPECT_TRUE(x->get() == 0 && y->get() == 2);
    EXPECT_TRUE(x->use_count() == 1 && y->use_count() == 1);
    EXPECT_TRUE(x.get() != y.get());
    EXPECT_TRUE(y_ptr == y.get());

    // shadow copy
    {
        // move y to mut, y is moved, because x and y are not shared, y is shadow copy of mut
        // and its address is the same as mut
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        EXPECT_TRUE(y.get() == nullptr);

        mut->set(3);
        EXPECT_TRUE(x->get() == 0 && mut->get() == 3);
        EXPECT_TRUE(x->use_count() == 1 && mut->use_count() == 1);
        EXPECT_TRUE(x.get() != mut.get());

        // y is shadow copy of mut, its address is the same as mut
        y = std::move(mut);
        EXPECT_TRUE(y_ptr == y.get());
    }
    EXPECT_TRUE(x->get() == 0 && y->get() == 3);
    EXPECT_TRUE(x->use_count() == 1 && y->use_count() == 1);
    EXPECT_TRUE(x.get() != y.get());
    EXPECT_TRUE(y_ptr == y.get());

    // deep copy
    {
        // y is not moved, it is a mutate of y
        MutableColumnPtr mut = IColumn::mutate(y);
        mut->set(3);
        EXPECT_TRUE(x->get() == 0 && mut->get() == 3);
        EXPECT_TRUE(x->use_count() == 1 && mut->use_count() == 1);
        EXPECT_TRUE(y.get() != mut.get());

        y = std::move(mut);
        EXPECT_TRUE(y_ptr != y.get());
    }
    EXPECT_TRUE(x->get() == 0 && y->get() == 3);
    EXPECT_TRUE(x->use_count() == 1 && y->use_count() == 1);
    EXPECT_TRUE(x.get() != y.get());
    EXPECT_TRUE(y_ptr != y.get());
}

TEST_F(CowTest, TestColumnMutate1) {
    ConcreteColumn::MutablePtr x = ConcreteColumn::create(1);
    // ensure x is not used again.
    auto y = (std::move(*x)).mutate();
    y->set(2);
    EXPECT_EQ(2, x->get());
    EXPECT_EQ(2, y->get());
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());

    // if x changed, y will changed again.
    x->set(3);
    EXPECT_EQ(3, x->get());
    EXPECT_EQ(3, y->get());

    auto z = (std::move(*x)).mutate();
    z->set(4);
    EXPECT_EQ(3, x->get());
    EXPECT_EQ(3, y->get());
    EXPECT_EQ(4, z->get());
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());
    EXPECT_EQ(1, z->use_count());
}

TEST_F(CowTest, TestColumnMutate2) {
    const ConcreteColumn::Ptr x = ConcreteColumn::create(1);
    // ensure x is not used again.
    auto y = (std::move(*x)).mutate();
    y->set(2);
    EXPECT_EQ(2, x->get());
    EXPECT_EQ(2, y->get());
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());

    // x can't be changed, because x is const
    // x->set(3);

    auto z = (std::move(*x)).mutate();
    z->set(3);
    EXPECT_EQ(2, x->get());
    EXPECT_EQ(2, y->get());
    EXPECT_EQ(3, z->get());
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());
    EXPECT_EQ(1, z->use_count());
}

TEST_F(CowTest, TestConcreteVectorColumnMutate1) {
    ConcreteVectorColumn::MutablePtr x = ConcreteVectorColumn::create({1, 2, 3});
    // ensure x is not used again.
    auto y = (std::move(*x)).mutate();
    y->set(0, 2);

    EXPECT_EQ(2, x->get(0));
    EXPECT_EQ(2, y->get(0));
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());

    // if x changed, y will changed again.
    x->set(1, 3);
    EXPECT_EQ(3, x->get(1));
    EXPECT_EQ(3, y->get(1));

    auto z = (std::move(*x)).mutate();
    z->set(2, 4);
    EXPECT_EQ(3, x->get(2));
    EXPECT_EQ(3, y->get(2));
    EXPECT_EQ(4, z->get(2));
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());
    EXPECT_EQ(1, z->use_count());
}

TEST_F(CowTest, TestConcreteVectorColumnMutate2) {
    const ConcreteVectorColumn::Ptr x = ConcreteVectorColumn::create({1, 2, 3});
    // ensure x is not used again.
    auto y = (std::move(*x)).mutate();
    y->set(0, 2);
    EXPECT_EQ(2, x->get(0));
    EXPECT_EQ(2, y->get(0));
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());

    // x can't be changed, because x is const
    // x->set(3);

    auto z = (std::move(*x)).mutate();
    z->set(1, 3);
    EXPECT_EQ(2, x->get(1));
    EXPECT_EQ(2, y->get(1));
    EXPECT_EQ(3, z->get(1));
    EXPECT_EQ(2, x->use_count());
    EXPECT_EQ(2, y->use_count());
    EXPECT_EQ(1, z->use_count());
}

} // namespace starrocks