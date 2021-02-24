import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIMapper {
    public static void main(String[] args) {
        Registry r = null;

        try {
            r = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Mapper mapper = new Mapper(args[0]);
            assert r != null;
            r.bind("mapper", mapper);
            System.out.println("Mapper server ready");
        } catch (Exception e) {
            System.out.println("Mapper server main " + e.getMessage());
        }
    }
}
