import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;


public class CassandraFun {
private final CqlSession session = CqlSession.builder().withKeyspace("samplekey").build();

    public CqlSession getSession() {
        return session;
    }

    public void makTable(){
        PreparedStatement preparedStatement = session.prepare("Create table if not exists XML(id int primary key ,name text,email text,age int)");
        session.execute(preparedStatement.bind());
    }
    public Boolean addtoCass(User user){
        PreparedStatement preparedStatement = session.prepare("Insert into samplekey.xml(id,name,email,age)values("+user.getId()+",'"+user.getName()+"','"+user.getEmail()+"',"+user.getAge()+")");
        session.execute(preparedStatement.bind());


        return true;
    }
}
