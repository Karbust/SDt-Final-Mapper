import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Mapper extends UnicastRemoteObject implements MapperMasterInterface {
    private final StorageMapperInterface smi;
    private final String id;

    public Mapper(String id) throws RemoteException, MalformedURLException, NotBoundException {
        smi = (StorageMapperInterface) Naming.lookup("rmi://localhost:2025/storage");
        this.id = id;
    }

    @Override
    public void calculateCombinations(int len, int fileCount, ArrayList<String> reducersList) throws RemoteException, InterruptedException {
        LinkedHashMap<String, ArrayList<ResourceInfo>> timeHarMap = smi.getTimeHarMap();
        if (timeHarMap == null) { return; }

        List<ProcessCombinationModel> PCMList = new ArrayList<>(); //Process Combination Model ID's
        ArrayList<String> resources = new ArrayList<>(timeHarMap.keySet());
        System.out.println("Número resources " + resources.size());
        Set<Set<String>> combinations = Sets.combinations(ImmutableSet.copyOf(resources), len);

        Set r;
        StringBuilder line = new StringBuilder();
        Iterator combIterator = combinations.iterator();
        System.out.println("Número combinações " + combinations.size());
        while (combIterator.hasNext()) {
            r = (Set) combIterator.next();
            line.setLength(0);
            Iterator lineIterator = r.iterator();
            while (lineIterator.hasNext()) {
                if (line.length() > 0) line.append(",");
                line.append(lineIterator.next().toString());
            }

            ProcessCombinationModel combinationInfo = new ProcessCombinationModel();
            combinationInfo.combination = line.toString();

            PCMList.add(combinationInfo);
        }

        if (PCMList.isEmpty()) return;

        List<List<ProcessCombinationModel>> lista = Lists.partition(PCMList, (int)Math.ceil(PCMList.size()/(float)reducersList.size()));

        HashMap<String, ArrayList<ProcessCombinationModel>> combinationsMap = new HashMap<>(); // < Reducer ID, List<Combination> >

        for (int i = 0; i < lista.size(); i++) {
            combinationsMap.put("Reducer" + reducersList.get(i), new ArrayList<>(lista.get(i)));
        }

        smi.saveCombinationsMapper(id, combinationsMap);

        List<Thread> threadList = new ArrayList<>();

        for(int i = 0; i < lista.size(); i++) {
            int finalI = i;
            String url = "rmi://localhost:"+ reducersList.get(i) +"/reducer";
            Thread t = (new Thread(() -> {
                try {
                    ReducerMapperInterface rmi = (ReducerMapperInterface) Naming.lookup(url);
                    rmi.calculateStatistics(this.id, "Reducer" + reducersList.get(finalI), fileCount);
                } catch (RemoteException | NotBoundException | MalformedURLException e) {
                    e.printStackTrace();
                }
            }));
            t.start();
            threadList.add(t);
        }
    }
}
