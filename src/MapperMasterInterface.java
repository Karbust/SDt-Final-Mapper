import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface MapperMasterInterface extends Remote {
    void calculateCombinations(int len, int fileCount, ArrayList<String> reducersList) throws RemoteException, InterruptedException;
}
