package common;

/**
 * Created by Administrator on 2017/2/6.
 */
public class Person {
    public Person(String _name, int _age, String _sex) {
        name=_name;
        age=_age;
        sex=_sex;
    }

    private String name;
    private int age;
    private String sex;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name=name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age=age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex=sex;
    }
}
