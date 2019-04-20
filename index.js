
const smartX = ( IPFS , ORBITDB ) => {

    if (IPFS)
        Ipfs = IPFS

    if (ORBITDB)
        OrbitDB = ORBITDB


// Create IPFS instance
    const ipfs = new Ipfs ( {
            start : true ,
            EXPERIMENTAL : {
                pubsub : true ,
            } ,
            config : {
                Addresses : {
                    Swarm : [
                        // Use IPFS dev signal server
                        // '/dns4/star-signal.cloud.ipfs.team/wss/p2p-webrtc-star',
                        '/dns4/ws-star.discovery.libp2p.io/tcp/443/wss/p2p-websocket-star' ,
                        //      '/dns4/ws-star-signal-2.servep2p.com/tcp/443/wss/p2p-websocket-star',
                        // Use local signal server//
                        // '/ip4/0.0.0.0/tcp/9090/wss/p2p-webrtc-star',
                    ]
                } ,
                Bootstrap: [
                    "/ipfs/QmWErSJMrwzrW6s8MLdjtzRQPPzzGvvjmqATsuT8s1NvYX"
                ]
            } ,
        } ,
    )

    ipfs.on ( 'error' , ( e ) => {console.log('some error', e)} )
    ipfs.on ( 'ready' , async () => {

        const orbitdb = new OrbitDB ( ipfs )

        const myAccount = await orbitdb.open ( 'account' , {
            create : true ,
            overwrite : true ,
            localOnly : false ,
            type : 'keyvalue' ,
        } )
        await myAccount.load ()
        console.log('My account: ', Object.entries(myAccount)[ 13 ][ 1 ][ '_index' ])

        const myAddress = await myAccount.address.toString ()
        const mySmartID = await OrbitDB.parseAddress ( myAddress ).root

        await orbitdb._pubsub.subscribe ( 'smartX' , pendingChange , onPeer )

        orbitdb._pubsub._subscriptions[ 'smartX' ].topicMonitor.on ( 'leave' , async ( peer ) => {
            const smartID = await peerTosmartID(peer)

            ipfs.pubsub.peers ( 'smartX' ).then ( async ( peers ) => {
                console.log(`all peers: `, peers)
                console.log ( `${smartID} left` )
            } )
        } )

        let publicAccount;
        let oracleSmartID;

        const publicSmartID = 'QmTmTRkTQB4vp6wBmWenFNsAQHovGsJVmLPQrdrjKYv6dB'
        const publicPeerID = 'QmWErSJMrwzrW6s8MLdjtzRQPPzzGvvjmqATsuT8s1NvYX'

        async function pendingChange( topic, data ) {
            oracleSmartID = data.oracleSmartID
            const publicAccountkey = '04a06c7212e67b42eb52cfa151223df06b5b0b4b9dd7c9da004e66e4dde2a202ea0a12963d3ab635c2c32253154f74ef89bbb1e7b6cc10c40219cc0b373c77be64'
            const pubKey = await orbitdb.keystore.importPublicKey( publicAccountkey )

            if (data.by === publicSmartID && await orbitdb.keystore.verify( data.signature , pubKey , data.entryHash )) {

                if (data.type === 'publicEntries' && !publicAccount) {
                    publicAccount = data.entry
                    console.log( `public account entries: `, publicAccount )
                }
                else if ( data.to === mySmartID || data.to === myAccount.get( 'social' ).twitter ) {
                    console.log( `there are some pending changes for...` , mySmartID , data )

                    if (data.type === 'verificationRequestedFromFriend') {
                        const arr = myAccount.get( 'friendVerificationRequests' )
                        console.log( `received verification request from: ${data.from}` )
                        if (arr.find( x => x.smartID === data.from ) === undefined) {
                            arr.push( {
                                smartID : data.from ,
                                ID : myAccount.get( 'state' ).friendVerificationRequests
                            } )
                            myAccount.get( 'state' ).friendVerificationRequests = await myAccount.put( 'friendVerificationRequests' , arr )
                            await myAccount.put( 'state' , myAccount.get( 'state' ) )
                                .then( async ( hash ) => await sendStateToPublicAccount( hash ).then( () => {
                                    console.log( 'added your new account to public account' )
                                } ) )
                            console.log( 'pending friend requests: ' , arr )
                            await friendVerificationRequest( data.from )
                        }
                    } else if (data.type === 'verifiedByFriend') {
                        await verifiedByFriend( data )
                        console.log( `verified by Friend: ${data.from}` )
                    } else if (data.type === 'verifiedByOracle') {
                        await verifiedByOracle( data )
                        console.log( `verified by Oracle: ${data.from}` )
                    } else if (data.type === 'rewardForVerification') {
                        if (mySmartID !== oracleSmartID) {
                            await reward( data )
                            console.log( `rewarded for verifying: ${data.from}` )
                        }
                    } else if (data.type === 'verificationRejectedByFriend') {
                        await rejectedByFriend( data )
                        console.log( `rejected by friend: ${data.from}` )
                    } else if (data.type === 'verificationRejectedByOracle') {
                        await rejectedByOracle( data )
                        console.log( `rejected by Oracle: ${data.from}` )
                    } else if (data.type === 'penaltyForVerification') {
                        if (mySmartID !== oracleSmartID) {
                            await penalty( data )
                            console.log( `lost for wrongly verifying: ${data.from}` )
                        }
                    } else if (data.type === 'rewardForChallenge') {
                        await challengerReward( data )
                        console.log( `won for successfully challenging: ${data.from}` )
                    } else if (data.type === 'challengeResolved') {
                        await challengeResolved( data )
                        console.log( `challenge resolved for: ${data.from}` )
                    } else if (data.type === 'rejectedByPeer') {
                        if (mySmartID !== oracleSmartID) {
                            await rejectedByPeer( data )
                            console.log( `challenged by peer: ${data.from}` )
                        } else {
                            await networkVerificationRequest( data.from )
                            console.log( `received verification request from: ${data.from}` )
                        }
                    } else if (data.type === 'transaction') {
                        await receiveValue( data.entry )
                        console.log( `received money from: ${data.from}` )
                    } else if (data.type === 'token') {
                        await sendValue( data.entry.to , data.entry.amount , data.entry.message , data.entry.unit )
                        console.log( `bought/sold token: ${data.entry.to}` )
                    } else if (data.type === 'twitterTip') {
                        await sendValue( data.entry.to , data.entry.amount , data.entry.message , data.entry.unit );
                        console.log( `sent tip on twitter to: ${data.entry.to}` )
                    } else if (data.type === 'submittedToNetwork' && mySmartID !== data.entry.verifyingPeer) {
                        await networkVerificationRequest( data.from )
                        console.log( `received verification request from: ${data.from}` )
                    } else if (data.type === 'twitterVerification') {
                        await updateTwitter( data.entry.twitter , data.entry.name , data.entry.bio );
                        console.log( `updated twitter handle and added video proof: ${data.entry.twitter}` )
                        if (data.entry.verifyingPeer !== mySmartID) {
                            await submitForVerification( data.entry.verifyingPeer )
                            console.log( 'checking account for verification...' )
                        }
                    }
                }
            }
        }

        async function checkAccount () {
            if (publicAccount.index) {
                oracleSmartID = publicAccount.socialServices.oracles[ 0 ]
                console.log('oracleSmartID: ', oracleSmartID)
                if (publicAccount.index[mySmartID] === undefined) {
                    console.log('peerID-smartID mapping not present so adding...')
                    let dataObj = {
                        from : mySmartID ,
                        fromPeer : orbitdb.id ,
                        to : publicSmartID ,
                        type : 'index' ,
                    }
                    await orbitdb._pubsub.publish( 'smartX' , dataObj )
                }
                if (myAccount.get( 'smartID' ) === undefined && publicAccount[mySmartID] === undefined) {
                    console.log( 'Account does not exist for smartID: ' , mySmartID )
                    await createAccount()
                } else {console.log('Account already exists for this smartID: ', mySmartID)}
            } else {console.log('Public account not loaded yet. Index: ', publicAccount.index)}
        }

        async function sendStateToPublicAccount (hash) {

            await myAccount.load()

            //Object.values(myAccount.get('state')).forEach(value => console.log(unHash(value)))

            let dataObj = {
                entries: myAccount.get('state'),
                to: publicSmartID,
                from: mySmartID,
                type: 'newEntry'
            }

            ipfs.pubsub.peers('smartX').then( async (peers) => {
                if (peers.find((x) => x === publicPeerID ) !== undefined ) {
                    await orbitdb._pubsub.publish ( 'smartX' , dataObj )
                    console.log ( 'state changes propagated successfully to public account. Own account entries: ', hash )
                } else {
                    let pendingChanges = myAccount.get('pendingChanges')
                    pendingChanges.push(dataObj)
                    await myAccount.put('pendingChanges', pendingChanges)
                    console.log('state changes saved as pending', hash)
                }
            })
        }

        async function peerTosmartID (peer) {
            let smartID;
            if (peer !== publicPeerID) {
                let index = publicAccount.index;
                for (let x in index) {
                    if (index[ x ].peers === peer) {
                        return smartID = x
                    }
                }
                return smartID
            } else if (peer === publicPeerID) {
                smartID = publicSmartID
            }
            return smartID
        }

        async function onPeer (topic, peer) {
            console.log('new peer joined')

            setTimeout (async () => {
                if (publicAccount) {
                    const smartID = await peerTosmartID( peer )
                    console.log( 'peer: ' , peer , ' smartID: ' , smartID )
                    smartID === publicSmartID ? console.log( `connected to public account: ${publicSmartID}` ) :
                        console.log( `welcome to ${topic}: ${smartID}` )

                    if (smartID === publicSmartID && myAccount.get( 'pendingChanges' ) !== undefined &&
                        myAccount.get( 'pendingChanges' ).length >= 1) {
                        console.log( 'there are pending changes for public account...' )
                        const pendingChanges = myAccount.get( 'pendingChanges' )
                        await pendingChanges.forEach( async dataObj => await orbitdb._pubsub.publish( 'smartX' , dataObj ) )
                        console.log( 'pending state changes propagated successfully to public account' , pendingChanges )
                        const newPendingChanges = []
                        await myAccount.put( 'pendingChanges' , newPendingChanges )
                    }

                    ipfs.pubsub.peers( 'smartX' ).then( async ( peers ) => {
                        const smartIDs = [];
                        console.log( `all peers: ` , peers )
                        peers.forEach( async ( peer ) => smartIDs.push( await peerTosmartID( peer ) ) )
                    } )
                }
            }, 60000)
        }

        async function createAccount () {
            await myAccount.load()

            let hashes = {}

            hashes.smartID = await myAccount.put( 'smartID' , mySmartID );

            hashes.peerID = await myAccount.put( 'peerID' , await orbitdb.id );

            hashes.signature = await myAccount.put( 'signature' , await orbitdb.keystore.sign( orbitdb.key , myAccount.get( 'smartID' ) ) );

            hashes.proof = await myAccount.put( 'proof' , null );

            hashes.name = await myAccount.put( 'name' , null );

            hashes.bio = await myAccount.put( 'bio' , null );

            hashes.social = await myAccount.put( 'social' , {twitter: ''} );

            hashes.friendVerificationRequests = await myAccount.put( 'friendVerificationRequests' , [ { smartID : '' , } ] );

            /*if (await publicAccount.get('verifiedMembers').members === 0) {
                hashes.verifyingPeer = await myAccount.put( 'verifyingPeer' , {
                    smartID : '' ,
                    securityDeposit : '' ,
                    pendingReward : '' ,
                    status : 'pending'
                } );

              //  console.log( 'As genesis peer, your account is automatically verified' )
            } else {
                hashes.verifyingPeer = await myAccount.put( 'verifyingPeer' , {
                    smartID : '' ,
                    securityDeposit : '' ,
                    pendingReward : '' ,
                    status : 'pending'
                } );

            }*/

            hashes.verifyingPeer = await myAccount.put( 'verifyingPeer' , {
                smartID : '' ,
                securityDeposit : '' ,
                pendingReward : '' ,
                status : 'pending'
            } );

            hashes.peersVerified = await myAccount.put( 'peersVerified' , [ {
                smartID : '' ,
                securityDeposit : '' ,
                pendingReward : '' ,
                status : '' ,
                timestamp: '',
                } ]
            );

            hashes.peersRejected = await myAccount.put( 'peersRejected' , [ {
                smartID : '' ,
                reason : '' ,
                status : '' ,
                timestamp: '',
                } ]
            );

            hashes.rejectingPeer = await myAccount.put( 'rejectingPeer' , {
                smartID : '' ,
                reason : '' ,
                status : '' ,
            } )

            hashes.transactions = await myAccount.put( 'transactions' , [ {
                from : '' ,
                to : '' ,
                amount : 0 ,
                message : '' ,
                type : 'debit' ,
                unit: '',
                timestamp: ''
                } ]
            );

            hashes.accountType = await myAccount.put( 'accountType' , 'user' );

            hashes.pendingChanges = await myAccount.put( 'pendingChanges' , [ {
                entries: '',
                to: '',
                from: '',
                type: '',
                } ]
            );

            //console.log('new account hashes: ', hashes)

            console.log( 'new account created for smartID: ' , mySmartID, Object.entries( myAccount )[ 13 ][ 1 ][ '_index' ] )

            /*let dataObj = {
                from : mySmartID ,
                fromPeer : orbitdb.id ,
                to : publicSmartID ,
                type : 'index' ,
            }

            await orbitdb._pubsub.publish ( 'smartX' , dataObj )*/

            await myAccount.put('state', hashes).then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                console.log('added your new account to public account')
            }))

            //setTimeout(() => location.reload(true), 15000)
        }

        async function minting () {
            await myAccount.load()

            if (myAccount.get('verifyingPeer') !== undefined &&
                myAccount.get( 'verifyingPeer' ).status === 'verified' &&
                publicAccount.index[mySmartID].memberNumber !== undefined && publicAccount.index[mySmartID].createdAt !== undefined ) {
                //console.log('member number: ', publicAccount.get('index')[mySmartID].memberNumber)

                const firstMemberIssued = 108573;
                const memberNumber = publicAccount.index[mySmartID].memberNumber
                const totalIssued = myAccount.get( 'verifyingPeer' ).status === 'verified' ?
                    firstMemberIssued * Math.pow( (1 - 1 / Math.pow( 10 , 10 )), memberNumber) : 0

                const actualIssued = totalIssued - myAccount.get( 'verifyingPeer' ).pendingReward
                const vestingDays = actualIssued / firstMemberIssued * 5 * 365;
                const dailyBonus = actualIssued / vestingDays
                const daysElapsed = Math.ceil( (Date.now() - publicAccount.index[mySmartID].createdAt) / (1000 * 60 * 60 * 24) )

                const minted = daysElapsed <= vestingDays ? .01*actualIssued + (daysElapsed / vestingDays * 0.99 * actualIssued) : actualIssued
                return {vested: minted, issued: actualIssued, dailyBonus: dailyBonus}
            } else {
                console.log('account not yet created or verified')
                const minted = 0
                return {vested: minted, issued: 0, dailyBonus: 0}
            }
        }

        async function smartCoinBalance() {
            await myAccount.load()

            const alreadyMinted = await minting()
            const minted = alreadyMinted.vested
            //console.log('minted: ', minted )

            const pending = myAccount.get( 'peersVerified' ).filter( x => x.status === 'pending' )
            const rewarded = myAccount.get( 'peersVerified' ).filter( x => x.status === 'verified' )
            const lost = myAccount.get( 'peersVerified' ).filter( x => x.status === 'rejected' )
            const challenged = myAccount.get( 'peersRejected' ).filter( x => x.status === 'rejected' )
            const smartCoinTransactions = myAccount.get('transactions').filter(x => x.unit === 'smartCoin')

            const securityDeposit = pending.reduce( ( total , transaction ) => total + transaction.securityDeposit , 0 )
            //console.log( `securityDeposit: ${securityDeposit}` )
            const pendingReward = pending.reduce( ( total , transaction ) => total + transaction.pendingReward , 0 )
            //console.log( `pendingReward: ${pendingReward}` )
            const rewardWon = rewarded.reduce( ( total , transaction ) => total + transaction.pendingReward , 0 )
            //console.log( `rewardWon: ${rewardWon}` )
            const securityLost = lost.reduce( ( total , transaction ) => total + transaction.securityDeposit , 0 )
            //console.log( `securityLost: ${securityLost}` )
            const challengeReward = challenged.reduce( ( total , transaction ) => total + transaction.reward , 0 )
            //console.log( `challengeReward: ${challengeReward}` )
            const transactionsBalance = smartCoinTransactions.reduce( ( total , transaction ) => total + transaction.amount , 0 )
            //console.log( `transactionsBalance: ${transactionsBalance}` )

            const smartCoin = minted - securityDeposit + rewardWon - securityLost + challengeReward + transactionsBalance
            //console.log( 'smartCoin: ' , smartCoin )
            return smartCoin
        }

        async function taxDistribution () {
            await myAccount.load()

            const alreadyMinted = await minting()
            const minted = alreadyMinted.vested
            const taxDeductionOnMinted = 0.15 * minted

            const royaltyIncome = myAccount.get( 'transactions' ).filter(x => x.message.includes('royalty fee') && x.type === 'credit').reduce( ( total , transaction ) => total + transaction.amount , 0 )
            const taxDeductionOnRoyaltyIncome = royaltyIncome ? 0.15 * royaltyIncome : 0
            const tipIncome = myAccount.get( 'transactions' ).filter(x => x.message.includes('tip on twitter') && x.type === 'credit').reduce( ( total , transaction ) => total + transaction.amount , 0 )
            const taxDeductionOnTipIncome = tipIncome ? 0.15 * tipIncome : 0

            const taxTransactions = myAccount.get( 'transactions' ).filter(x => x.message.includes('tax paid to')).reduce( ( total , transaction ) => total + transaction.amount , 0 )
            const taxAlreadyPaid = taxTransactions ? Math.abs(taxTransactions) : 0
            let taxToBePaid = taxDeductionOnMinted + taxDeductionOnRoyaltyIncome + taxDeductionOnTipIncome - taxAlreadyPaid

            if (taxToBePaid > 100) {
                console.log(
                    `taxDeductionOnMinted: ${taxDeductionOnMinted}, 
                    taxDeductionOnRoyaltyIncome: ${taxDeductionOnRoyaltyIncome}, 
                    taxDeductionOnTipIncome: ${taxDeductionOnTipIncome},
                    taxAlreadyPaid: ${taxAlreadyPaid}, 
                    taxToBePaid: ${taxToBePaid}`
                )

                const socialServices = publicAccount.socialServices
                const taxPerService = taxToBePaid / (Object.entries(socialServices).length)
                console.log(Object.values(socialServices))

                for (let i=0; i < Object.values(socialServices).length; i++) {
                    let service = Object.values(socialServices)[i]
                    let taxPerServiceProvider = taxPerService / service.length
                    for (let j=0; j < service.length; j++) {
                        let serviceProvider = service[j]
                        console.log(`service provider: `, serviceProvider, `taxPerServiceProvider: `, taxPerServiceProvider)
                        if (!serviceProvider || serviceProvider !== mySmartID) {
                            setTimeout (async () => await sendValue( serviceProvider , taxPerServiceProvider , `${Object.keys(socialServices)[i]}` + ' tax paid to ' + serviceProvider, 'smartCoin').then( () => {
                                console.log( 'tax credited to service provider account' )
                            } ), j*2000)
                        }
                    }
                }
                console.log( 'tax credited to all smartX social service providers' )
            } else {
                console.log( 'no dues at this time' )
            }
        }

        async function calculateDepositAndReward() {

            const recentMemberNumber = (publicAccount.verifiedMembers.members)
            const firstMemberIssued = 108573;
            const value = firstMemberIssued * Math.pow( (1 - 1 / Math.pow( 10 , 10 )), recentMemberNumber)
            const pendingReward = value * 0.005;
            const securityDeposit = 2 * pendingReward;

            return { securityDeposit : securityDeposit , pendingReward : pendingReward }
        }

        async function submitToFriend( smartID ) {

            await myAccount.load()

            myAccount.get('state').verifyingPeer = await myAccount.put ( 'verifyingPeer' , {
                smartID : smartID ,
                securityDeposit : 0 ,
                pendingReward : 0 ,
                status : 'pending',
                ID: myAccount.get('state').verifyingPeer,
            })

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log('request submitted to friend for verification')
            }))
        }

        async function verify( smartID ) {
            await myAccount.load()

            if (!await myAccount.get ( 'peersVerified' ).find(x => x.smartID === smartID) &&
                !await myAccount.get ( 'peersRejected' ).find(x => x.smartID === smartID)) {

                if (mySmartID !== oracleSmartID && await myAccount.get ( 'verifyingPeer' ).status === 'verified') {
                    calculateDepositAndReward ().then ( async ( x ) => {

                        if (await smartCoinBalance() >= x.securityDeposit) {

                            const arr = await myAccount.get ( 'peersVerified' )
                            arr.push ( {
                                smartID : smartID ,
                                securityDeposit : x.securityDeposit ,
                                pendingReward : x.pendingReward ,
                                status : 'pending' ,
                                timestamp: Date.now(),
                                ID: myAccount.get('state').peersVerified,
                            } )

                            console.log ( 'peers Verified: ', arr )

                            myAccount.get('state').peersVerified = await myAccount.put ( 'peersVerified' , arr )
                            await myAccount.put('state', myAccount.get('state') )
                                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                                console.log('verified a friend and deposited some smartCoin')
                            }))

                        } else {
                            console.log ( 'You dont have enough balance', await smartCoinBalance(), x.securityDeposit )
                        }
                    } )
                } else if (mySmartID === oracleSmartID) {

                    const arr = myAccount.get ( 'peersVerified' )
                    console.log ( arr )
                    arr.push ({
                        smartID: smartID,
                        securityDeposit: 0,
                        pendingReward: 0,
                        status: 'verified',
                        timestamp: Date.now(),
                        ID: myAccount.get('state').peersVerified,
                    } )

                    myAccount.get('state').peersVerified = await myAccount.put ( 'peersVerified' , arr )
                    await myAccount.put('state', myAccount.get('state'))
                        .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                            console.log('successfully verified a new peer')
                        }))

                } else {
                    alert('Not able to verify')
                }
            } else {
                alert('You have already verified or rejected this peer')
            }
        }

        async function reject( smartID ) {
            await myAccount.load()

            if (!await myAccount.get ( 'peersVerified' ).find(x => x.smartID === smartID) &&
                !await myAccount.get ( 'peersRejected' ).find(x => x.smartID === smartID)) {

                const verifyingPeerID = publicAccount[mySmartID]['verifyingPeer'].smartID
                const rejectingPeerID = publicAccount[mySmartID]['rejectingPeer'].smartID
                if (mySmartID !== oracleSmartID && mySmartID !== verifyingPeerID && !rejectingPeerID) {

                    const arr = await myAccount.get ( 'peersRejected' )
                    console.log ( arr )
                    arr.push ( {
                        smartID: smartID,
                        reason: '',
                        reward: 0,
                        status: 'pending',
                        timestamp: Date.now(),
                        ID: myAccount.get('state').peersRejected,
                    })

                    console.log ( 'peers Rejected: ', arr )

                    myAccount.get('state').peersRejected = await myAccount.put ( 'peersRejected' , arr )
                    await myAccount.put('state', myAccount.get('state'))
                        .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                            console.log('you rejected some network peer')
                        }))

                } else if (mySmartID === verifyingPeerID) {
                    const arr = await myAccount.get ( 'peersRejected' )
                    console.log ( arr )
                    arr.push ( {
                        smartID: smartID,
                        reason: '',
                        reward: 0,
                        status: 'rejected',
                        timestamp: Date.now(),
                        ID: myAccount.get('state').peersRejected,
                    })

                    myAccount.get('state').peersRejected = await myAccount.put ( 'peersRejected' , arr )
                    await myAccount.put('state', myAccount.get('state'))
                        .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                            console.log('you rejected friend')
                        }))

                } else if (mySmartID === oracleSmartID) {

                    const arr = await myAccount.get( 'peersRejected' )
                    console.log( arr )
                    arr.push( {
                        smartID : smartID ,
                        reason : '' ,
                        reward : 0 ,
                        status : 'rejected' ,
                        timestamp: Date.now(),
                        ID: myAccount.get('state').peersRejected,
                    } )

                    myAccount.get('state').peersRejected = await myAccount.put ( 'peersRejected' , arr )
                    await myAccount.put('state', myAccount.get('state'))
                        .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                            console.log('you rejected peer')
                        }))
                }
                console.log ( 'rejected ' + smartID + ' by ' + mySmartID )
            } else {
                alert('You have already verified or rejected this peer')
            }
        }

        async function verifiedByFriend( data ) {
            await myAccount.load()

            myAccount.get('state').verifyingPeer = await myAccount.put ( 'verifyingPeer' , {
                smartID : data.from ,
                securityDeposit : data.entry.securityDeposit ,
                pendingReward : data.entry.pendingReward ,
                status : 'pending',
                ID: myAccount.get('state').verifyingPeer,
            })

            await myAccount.put('state', myAccount.get('state') )
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log('friend verified you successfully')
            }))

            console.log ( 'verified ' + mySmartID + ' by Friend ' + data.from , myAccount.get ( 'verifyingPeer' ) + 'pushed to network' )
        }

        async function verifiedByOracle( data ) {
            await myAccount.load()

            /*await myAccount.put( 'createdAt' , Date.now() );

            await myAccount.put( 'memberNumber' , publicAccount.get('verifiedMembers').members + 1 );*/

            myAccount.get('state').verifyingPeer = await myAccount.put ( 'verifyingPeer' , {
                smartID : myAccount.get ( 'verifyingPeer' ).smartID ,
                securityDeposit : myAccount.get ( 'verifyingPeer' ).securityDeposit ,
                pendingReward : myAccount.get ( 'verifyingPeer' ).pendingReward ,
                status : 'verified' ,
                ID: myAccount.get('state').verifyingPeer,
            })

            if (myAccount.get ( 'rejectingPeer' ).smartID !== '' || null) {
                myAccount.get('state').rejectingPeer = await myAccount.put ( 'rejectingPeer' , {
                    smartID : myAccount.get ( 'rejectingPeer' ).smartID ,
                    reason: '',
                    status : 'verified' ,
                    ID: myAccount.get('state').rejectingPeer,
                })
                console.log ( 'verified ' + mySmartID + ' by Oracle ', 'rejectingPeer updated: ' , myAccount.get ( 'rejectingPeer' ) )
            }

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    smartCoinBalance().then((balance) => {
                        console.log ( 'verified ', mySmartID, ' by Oracle ', 'verifyingPeer updated: ', myAccount.get ( 'verifyingPeer' )  )
                        console.log('congrats for getting verified and receiving money. New balance: ', balance)
                    })
                }))
        }

        async function rejectedByPeer( data ) {
            await myAccount.load()

            myAccount.get('state').rejectingPeer = await myAccount.put ( 'rejectingPeer' , {
                smartID : data.from ,
                reason : data.entry.reason ,
                status : 'pending',
                ID: myAccount.get('state').rejectingPeer,
            } )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log ( 'challenged ' + mySmartID + ' by peer ' , data.from , myAccount.get ( 'rejectingPeer' ) )
            }))

        }

        async function rejectedByFriend( data ) {
            await myAccount.load()

            console.warn ( 'Your account was rejected by friend for ' , data.entry )

            myAccount.get('state').verifyingPeer = await myAccount.put ( 'verifyingPeer' , {
                smartID : data.from ,
                securityDeposit : 0 ,
                pendingReward : 0 ,
                status : 'rejected',
                ID: myAccount.get('state').verifyingPeer,
            } )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                console.log('updated friend rejection status')
            }))
        }

        async function rejectedByOracle( data ) {
            await myAccount.load()
            console.warn ( 'Your account was rejected by oracle for ' , data.entry )

            if (myAccount.get ( 'rejectingPeer' ).smartID !== '' || null) {
                myAccount.get( 'state' ).rejectingPeer = await myAccount.put( 'rejectingPeer' , {
                    smartID : myAccount.get( 'rejectingPeer' ).smartID ,
                    status : 'rejected' ,
                    securityDeposit : myAccount.get( 'verifyingPeer' ).securityDeposit ,
                    ID : myAccount.get( 'state' ).rejectingPeer ,
                } )
            }

            myAccount.get('state').verifyingPeer = await myAccount.put ( 'verifyingPeer' , {
                smartID : myAccount.get ( 'verifyingPeer' ).smartID ,
                securityDeposit : myAccount.get ( 'verifyingPeer' ).securityDeposit ,
                pendingReward : myAccount.get ( 'verifyingPeer' ).pendingReward ,
                status : 'rejected' ,
                ID: myAccount.get('state').verifyingPeer,
            } )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log ( 'rejected ' + mySmartID + ' by oracle ', 'verifyingPeer updated: ', myAccount.get ( 'verifyingPeer' ) )
                    console.log ( 'rejected ' + mySmartID + ' by Oracle ', 'rejectingPeer updated: ', myAccount.get ( 'rejectingPeer' ) )
            }))
        }

        async function reward( data ) {
            await myAccount.load()

            const arr = myAccount.get ( 'peersVerified' )

            arr[arr.findIndex(x => x.smartID === data.from)] = {
                smartID : data.from ,
                securityDeposit : data.entry.securityDeposit ,
                pendingReward : data.entry.pendingReward ,
                status : 'verified',
                timestamp: Date.now(),
                ID: myAccount.get('state').peersVerified,
            }

            myAccount.get('state').peersVerified = await myAccount.put ( 'peersVerified' , arr )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    smartCoinBalance().then((balance) => {
                        console.log('received reward for correct verification. New balance: ', balance)
                        console.log ( 'rewarded ' + mySmartID , myAccount.get ( 'peersVerified' ) )
                    })
            }))
        }

        async function penalty( data ) {
            await myAccount.load()

            const arr = await myAccount.get ( 'peersVerified' )

            arr[arr.findIndex(x => x.smartID === data.from)] = {
                smartID : data.from ,
                securityDeposit : data.entry.securityDeposit ,
                pendingReward: data.entry.pendingReward,
                status : 'rejected',
                timestamp: Date.now(),
                ID: myAccount.get('state').peersVerified,
            }

            console.log('array item updated', arr)

            myAccount.get('state').peersVerified = await myAccount.put ( 'peersVerified' , arr )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    smartCoinBalance().then((balance) => {
                        console.log('penalized for wrong verification. New balance: ', balance)
                        console.log ( 'penalized ' + mySmartID , myAccount.get ( 'peersVerified' ) )
                    })
                }))
        }

        async function challengerReward( data ) {
            await myAccount.load()

            const arr = myAccount.get ( 'peersRejected' )

            arr[arr.findIndex(x => x.smartID === data.from)] = {
                smartID : data.from ,
                reward : data.entry.securityDeposit,
                status: 'rejected',
                timestamp: Date.now(),
                ID: myAccount.get('state').peersRejected,
            }

            myAccount.get('state').peersRejected = await myAccount.put ( 'peersRejected' , arr )

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    smartCoinBalance().then((balance) => {
                        console.log('received reward for successful challenge. New balance: ', balance)
                    })
                }))
            console.log('array item updated', arr)
        }

        async function challengeResolved( data ) {
            await myAccount.load()

            const arr = await myAccount.get ( 'peersRejected' )

            arr[arr.findIndex(x => x.smartID === data.from)] = {
                smartID : data.from ,
                reward: 0,
                status : 'verified',
                timestamp: Date.now(),
                ID: myAccount.get('state').peersRejected,
            }

            myAccount.get('state').peersRejected = await myAccount.put ( 'peersRejected' , arr )

            await myAccount.put('state', myAccount.get('state') )
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log ( 'challenge resolved for ' + data.from )
                }) )

            console.log('array item updated', arr)
        }

        async function sendValue( smartID , amount , message, unit ) {
            await myAccount.load()

            const balance = unit === 'smartCoin' ? await smartCoinBalance() : await tokenBalance(mySmartID, unit)
            Number.isInteger(amount)

            if (balance >= Math.abs(amount) ) {

                const arr = myAccount.get ( 'transactions' )

                console.log ( arr )
                arr.push ( {
                    from : mySmartID ,
                    to : smartID ,
                    amount : - Math.abs ( amount ) ,
                    message : message ,
                    type : 'debit' ,
                    unit: unit,
                    timestamp: Date.now(),
                    ID: myAccount.get('state').transactions,
                } )

                myAccount.get('state').transactions = await myAccount.put ( 'transactions' , arr )

                await myAccount.put('state', myAccount.get('state'))
                    .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                        console.log ( 'Value debited' )
                    }))

            } else {
                console.log ( 'You dont have enough balance' )
            }

        }

        async function receiveValue( data ) {
            await myAccount.load()

            if (myAccount.get('transactions').find(x => x.timestamp === data.timestamp ) === undefined) {

                const arr = myAccount.get( 'transactions' )
                console.log( arr )
                arr.push( {
                    from : data.from ,
                    to : data.to ,
                    amount : Math.abs( data.amount ) ,
                    message : data.message ,
                    type : 'credit' ,
                    unit : data.unit ,
                    timestamp : data.timestamp ,
                    ID : data.ID ,
                } )

                myAccount.get( 'state' ).transactions = await myAccount.put( 'transactions' , arr )

                await myAccount.put( 'state' , myAccount.get( 'state' ) )
                    .then( async ( hash ) => await sendStateToPublicAccount( hash ).then( () => {
                        console.log( 'Value credited' )
                    } ) )
            }
        }

        async function tokenBalance (smartID, unit) {
            const tokenTransactions = publicAccount[mySmartID].transactions.filter(x => x.unit === unit)
            const balance = tokenTransactions.reduce( ( total , transaction ) => total + transaction.amount , 0 )
            console.log( `tokenBalance: ${balance}` )
            return balance
        }

        async function tokenSupply (smartID, unit) {
            const tokenTransactions = publicAccount[mySmartID].transactions.filter(x => x.unit === unit)
            const balance = tokenTransactions.reduce( ( total , transaction ) => total + transaction.amount , 0 )
            console.log( `tokenSupply: ${-balance}` )
            return -balance
        }
        
        async function updateTwitter (ID, name, bio) {

            myAccount.get('state').social = await myAccount.put ('social', {twitter : ID })
            myAccount.get('state').name = await myAccount.put ( 'name' , name)
            myAccount.get('state').bio = await myAccount.put ( 'bio' , bio)

            await myAccount.put('state', myAccount.get('state'))
                .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                    console.log('updated twitter handle, name and bio in public account')
                }))
        }

        async function forcedSync () {

                const accounts = Object.keys(publicAccount.index)
                accounts.forEach(async account => {
                    if (account !== mySmartID) {
                        const transactions = publicAccount[account].transactions
                        transactions.forEach(async transaction => {
                            if (transaction.to === mySmartID && myAccount.get('transactions').find(x => x.timestamp === transaction.timestamp) === undefined) {
                                console.log('transaction entry missing from account: ', transaction)
                                await receiveValue(transaction).then(() => console.log('transaction successfully credited'))
                            }
                        })
                        if (publicAccount[account].accountType === 'user'){
                            const peersVerified = publicAccount[account].peersVerified
                            peersVerified.forEach(async peerVerified => {
                                if (peerVerified.smartID === mySmartID && myAccount.get('verifyingPeer').smartID === account && account !== oracleSmartID) {
                                    console.log( 'peer verified entry missing from account: ' , peerVerified )
                                    await verifiedByFriend( peerVerified ).then( () => console.log( 'entry successfully added' ) )
                                } else if (peerVerified.smartID === mySmartID && myAccount.get('verifyingPeer').status === 'pending' && account === oracleSmartID) {
                                    console.log( 'oracle verified entry missing from account: ' , peerVerified )
                                    await verifiedByOracle( peerVerified ).then( () => console.log( 'entry successfully added' ) )
                                }
                            })
                            const peersRejected = publicAccount[account].peersRejected
                            peersRejected.forEach(async peerRejected => {
                                if (peerRejected.smartID === mySmartID && myAccount.get('verifyingPeer').smartID === account && account !== oracleSmartID) {
                                    console.log( 'friend rejected entry missing from account: ' , peerRejected )
                                    await rejectedByFriend( peerRejected ).then( () => console.log( 'entry successfully added' ) )
                                } else if (peerRejected.smartID === mySmartID && myAccount.get('verifyingPeer').smartID !== account && account !== oracleSmartID) {
                                    console.log( 'peer rejected entry missing from account: ' , peerRejected )
                                    await rejectedByPeer(peerRejected).then( () => console.log( 'entry successfully added' ) )
                                } else if (peerRejected.smartID === mySmartID && myAccount.get('verifyingPeer').smartID !== account && account === oracleSmartID) {
                                    console.log( 'oracle rejected entry missing from account: ' , peerRejected )
                                    await rejectedByOracle(peerRejected).then( () => console.log( 'entry successfully added' ) )
                                }
                            })
                        }
                    }
                })

        }

        async function unHash (hash) {
            return ipfs.object.get(hash, { enc: 'base58' })
                .then((obj) => JSON.parse(obj.toJSON().data))
                .then((data) => {
                    let entry = {
                        hash: hash,
                        id: data.id,
                        payload: data.payload,
                        next: data.next,
                        v: data.v,
                        clock: data.clock,
                    }
                    if (data.sig) Object.assign(entry, { sig: data.sig })
                    if (data.key) Object.assign(entry, { key: data.key })
                    return entry
                })
        }

        async function hashing (entry) {
            const data = Buffer.from(JSON.stringify(entry))
            const hash = await ipfs.object.put(data)
                .then((res) => res.toJSON().multihash)
            return hash
        }

        function integrate (start, end, step) {
            let total = 0
            step = step || 0.01
            for (let x = start; x < end; x += step) {
                total += Math.log10( 1 + x) * step
            }
            return total
        }

        async function twitterIDToSmartID (twitterID) {
            let smartID;
            let index = publicAccount.index;
            for (let x in index) {
                if (index[ x ].twitter === twitterID) {
                    return smartID = x
                }
            }
            return smartID
        }


        ////////////
        ////////////
        // HTML Specific Code
        ///////////
        ///////////

        async function openAccount (smartID) {

            console.log('opening account for smartID: ', smartID)

            const entries = smartID === mySmartID ? Object.entries(myAccount)[ 13 ][ 1 ][ '_index' ] : publicAccount[smartID]
            console.log( 'account entries: ', entries )

            document.getElementById('onboardingSmartID').value = mySmartID
            document.getElementById('onboardingSignature').value = await orbitdb.keystore.sign( orbitdb.key , mySmartID )
            document.getElementById('videoSmartID').value = mySmartID.substr(smartID.length - 4, 4)

            if (!entries) {
                document.getElementById('splashScreen').style.display = 'none'
                console.log('not ready yet...')
            } else {
                document.getElementById('splashScreen').style.display = 'none'
            }

            if (!entries || !entries.proof || !entries.social.twitter || entries.verifyingPeer === 'pending' && smartID === mySmartID ) {
                console.log('onboarding not completed yet...')
                document.getElementById('onboardingOverlay').style.display = 'none'
                /*let titleText = ['tweets', 'memes', 'videos', 'blogs', 'music', 'games', 'letters', 'podcasts', 'anything']
                let title = document.getElementById('titleText')
                let i = 0;
                setInterval(() => {
                    title.removeChild( title.childNodes[ 0 ] );
                    title.appendChild( document.createTextNode(titleText[i]) )
                    i === titleText.length - 1 ? i = titleText.length - 1 : i++ }, 2000)*/
            } else {
                document.getElementById('onboardingOverlay').style.display = 'block'
                document.getElementById('onboardingModal').style.display ='none'
                document.getElementById("profileOpen").click()
            }
            
            if (entries) {

                document.getElementById( 'smartID' ).value = smartID

                document.getElementById( 'signature' ).value = smartID === mySmartID ?
                    await orbitdb.keystore.sign( orbitdb.key , mySmartID ) :
                    await entries.signature
                try {
                    if (Object.entries( entries ).length !== 0) {

                        async function b64toBlob () {
                            let base64 = atob( entries.proof )
                            let ab = new ArrayBuffer( base64.length );
                            let ia = new Uint8Array( ab );
                            for (let i = 0; i < base64.length; i++) {
                                ia[ i ] = base64.charCodeAt( i );
                            }
                            return new Blob( [ ab ] , { type : 'video/x-matroska'} );
                        }

                        b64toBlob().then( ( blob ) => document.querySelectorAll( 'video' )[1].src = URL.createObjectURL( blob ) )
                        document.querySelectorAll( 'video' )[1].controls = "true"

                        if (entries.verifyingPeer.status === 'verified') {
                            document.getElementById( 'verifiedBy' ).setAttribute( "class" , "check i" )
                        } else {
                            document.getElementById( 'verifiedBy' ).setAttribute( "class" , "uncheck i" )
                        }

                        document.getElementById( 'name' ).value = entries.name

                        document.getElementById( 'bio' ).value = entries.bio

                        document.getElementById( 'twitter' ).href = await socialUrl(smartID)
                        document.getElementById( 'twitter' ).style.color = 'orange'

                        if (smartID === mySmartID && publicAccount) {

                            await taxDistribution()

                            let balance = await smartCoinBalance()
                            document.getElementById( '_balance' ).value = '💲' + balance.toFixed( 2 )
                            const pending = entries.peersVerified.filter( x => x.status === 'pending' );
                            const securityDeposit = pending.reduce( ( total , transaction ) => total + transaction.securityDeposit , 0 )
                            const pendingReward = pending.reduce( ( total , transaction ) => total + transaction.pendingReward , 0 )
                            document.getElementById( '_deposit' ).value = '💲' + securityDeposit.toFixed( 2 )
                            document.getElementById( '_reward' ).value = '💲' + pendingReward.toFixed( 2 )

                            const alreadyMinted = await minting()
                            document.getElementById( '_issued' ).value = alreadyMinted.vested > 0 ?
                                '💲 ' + alreadyMinted.issued.toFixed( 2 ) : '💲' + 0
                            document.getElementById( '_dailyBonus' ).value = alreadyMinted.vested > 0 ?
                                '💲 ' + alreadyMinted.dailyBonus.toFixed( 2 ) : '💲' + 0

                            await myAccount.get( 'transactions' ).forEach( ( transaction ) => {
                                if (transaction.to) {
                                    let i = document.createElement( 'tr' )
                                    let l = document.createElement( 'td' )
                                    let j = document.createElement( 'td' )
                                    let k = document.createElement( 'td' )
                                    i.setAttribute( "id" , transaction.timestamp )
                                    let txnDate = new Date( transaction.timestamp )
                                    l.appendChild( document.createTextNode( txnDate.toDateString() ) )
                                    j.appendChild( document.createTextNode( transaction.message ) )
                                    k.appendChild( document.createTextNode( transaction.unit === 'smartCoin' ? '💲' + transaction.amount.toFixed( 2 ) : transaction.amount.toFixed( 2 ) ) )
                                    i.appendChild( l )
                                    i.appendChild( j )
                                    i.appendChild( k )
                                    document.getElementById( 'transactions' ).appendChild( i )
                                }
                            } )
                        }
                    }
                } catch ( e ) {
                    console.error( e )
                }

                if (document.getElementById( "verification" )) {
                    document.getElementById( "verification" ).remove()
                } else if (document.getElementById( "verify" ) || document.getElementById( "reject" )) {
                    document.getElementById( "verify" ).remove()
                    document.getElementById( "reject" ).remove()
                }

                if (smartID !== mySmartID && (entries.verifyingPeer.smartID === mySmartID || mySmartID === oracleSmartID)
                    && !myAccount.get( 'peersVerified' ).find( x => x.smartID === smartID ) &&
                    !myAccount.get( 'peersRejected' ).find( x => x.smartID === smartID )) {
                    let i = document.createElement( 'button' )
                    i.setAttribute( "id" , "verify" )
                    i.appendChild( document.createTextNode( "Verify" ) )
                    document.getElementById( 'accountAction' ).appendChild( i )
                    document.getElementById( 'videoButton' ).style.display = 'none'

                    let j = document.createElement( 'button' )
                    j.setAttribute( "id" , "reject" )
                    j.appendChild( document.createTextNode( "Reject" ) )
                    document.getElementById( 'accountAction' ).appendChild( j )
                    document.getElementById( 'videoButton' ).style.display = 'none'

                    document.getElementById( 'reject' ).addEventListener( 'click' , () => {
                        reject( smartID )
                    } )
                    document.getElementById( 'verify' ).addEventListener( 'click' , () => {
                        verify( smartID )
                    } )

                } else if (smartID !== mySmartID &&
                    (entries.rejectingPeer !== undefined || entries.rejectingPeer.status !== 'verified') &&
                    !myAccount.get( 'peersVerified' ).find( x => x.smartID === smartID ) &&
                    !myAccount.get( 'peersRejected' ).find( x => x.smartID === smartID )) {
                    let j = document.createElement( 'button' )
                    j.setAttribute( "id" , "reject" )
                    j.setAttribute( "class" , "btn" )
                    j.setAttribute( "style" , "position: relative; bottom: 30px; width:200px" )
                    j.appendChild( document.createTextNode( "Reject" ) )
                    document.getElementById( 'accountAction' ).appendChild( j )
                    document.getElementById( 'videoButton' ).style.display = 'none'

                    document.getElementById( 'reject' ).addEventListener( 'click' , () => {
                        reject( smartID )
                    } )

                } else if (smartID === mySmartID) {
                    let i = document.createElement( 'button' )
                    i.setAttribute( "id" , "verification" )
                    i.setAttribute( "class" , "btn" )
                    i.setAttribute( "style" , "position: relative; bottom: 30px; width:200px" )
                    i.appendChild( document.createTextNode( "Update account" ) )
                    document.getElementById( 'accountAction' ).appendChild( i )
                    document.getElementById( 'verification' ).addEventListener( 'click' , updateAccount )
                }

            } else {
                console.log('account has not been loaded yet')
            }
        }

        async function submitForVerification (smartID) {
            const friendID = smartID
            if (friendID) {
                if (publicAccount.verifiedMembers.members >= 1) {

                    if (friendID === oracleSmartID && publicAccount.verifiedMembers.members >= 100) {
                        alert( "You can not directly request oracle to verify you. Your request could not be submitted." )
                    } else {
                        if (publicAccount.index[ friendID ].status === 'verified') {
                            if (myAccount.get('proof') !== null || '') {
                                await submitToFriend( friendID )
                                console.log( 'Verification request has been submitted to your friend.' )
                            } else {
                                alert( 'Not able to submit. Make sure you have recorded a video selfie.' )
                            }
                        } else {
                            alert( 'Request not submitted as chosen friend is not available or verified. ' +
                                'Please submit your request to a friend already verified' )
                        }
                    }
                }
            } else {
                    alert('the twitter ID you entered does not appear to have an account on smartX yet. Please check the twitter handle again')
            }
        }

        async function updateAccount() {
            if (myAccount.get('verifyingPeer').status === 'pending' || 'verified') {

                myAccount.get('state').name = await myAccount.put ( 'name' , document.getElementById('name').value)

                myAccount.get('state').bio = await myAccount.put ( 'bio' , document.getElementById('bio').value)

                await myAccount.put('state', myAccount.get('state'))
                    .then(async (hash) => await sendStateToPublicAccount(hash).then(() => {
                        console.log('updated name and bio in public account')
                    }))
            }
            else {
                alert ( 'Not allowed to update. Your account is already verified or under process' )
            }
        }

        async function displayRequests () {
            if (myAccount.get('friendVerificationRequests') !== undefined &&
                publicAccount.networkVerificationRequests !== undefined) {

                await myAccount.get( 'friendVerificationRequests' ).forEach( ( x ) => {
                    if (x.smartID) {
                        let i = document.createElement( 'button' )
                        i.setAttribute( "id" , x.smartID )
                        i.setAttribute( "value" , x.smartID )
                        i.setAttribute( "class" , "verificationRequests" )
                        i.appendChild( document.createTextNode( x.smartID ) )
                        document.getElementById( 'friendRequests' ).appendChild( i )
                        document.getElementById( x.smartID ).addEventListener( 'click' , () => {
                            openAccount( x.smartID ).then( () => {
                                console.log( 'friend request opened' )
                            } )
                        } )
                    }
                } )

                publicAccount.networkVerificationRequests.forEach( ( x ) => {
                    if (x.smartID !== undefined && x.status === 'verifiedByFriend' && mySmartID !== oracleSmartID) {
                        let i = document.createElement( 'button' )
                        i.setAttribute( "id" , x.smartID )
                        i.setAttribute( "value" , x.smartID )
                        i.setAttribute( "class" , "verificationRequests" )
                        i.appendChild( document.createTextNode( x.smartID ) )
                        document.getElementById( 'networkRequests' ).appendChild( i )
                        document.getElementById( x.smartID ).addEventListener( 'click' , () => {
                            openAccount( x.smartID ).then( () => {
                                console.log( 'network request opened' )
                            } )
                        } )
                    }
                    else if (x.smartID !== undefined && x.status === 'rejectedByNetwork' && mySmartID === oracleSmartID) {
                        let i = document.createElement( 'button' )
                        i.setAttribute( "id" , x.smartID )
                        i.setAttribute( "value" , x.smartID )
                        i.setAttribute( "class" , "verificationRequests" )
                        i.appendChild( document.createTextNode( x.smartID ) )
                        document.getElementById( 'networkRequests' ).appendChild( i )
                        document.getElementById( x.smartID ).addEventListener( 'click' , () => {
                            openAccount( x.smartID ).then( () => {
                                console.log( 'network request opened' )
                            } )
                        } )
                    }
                } )
            }
        }

        async function showTokens () {

            //get token accounts
            const tokenAccounts = []
            const index = publicAccount.index
            for (let x in index) {
                if (publicAccount[x].accountType === "token") {
                    tokenAccounts.push(x)
                }
            }
            console.log('token accounts: ', tokenAccounts)

            //for each token account get data and create chart listener

            tokenAccounts.forEach(async (tokenID) => {
                let i = document.createElement( 'button' )
                i.setAttribute( "id" , tokenID )
                i.setAttribute( "value" , tokenID )
                i.setAttribute( "class" , "tokenContainer" )
                i.appendChild( document.createTextNode( 'tokenID: ' + tokenID  ))
                let j = document.createElement('div')
                j.setAttribute('class', 'tweet')
                //console.log(publicAccount.get(tokenID).urlID)
                j.setAttribute('id', publicAccount[tokenID].urlID)
                i.appendChild(j)
                document.getElementById( 'tokensList' ).appendChild( i )

                document.getElementById( tokenID ).addEventListener( 'click' , async () => {
                    const dataArray = [['Time', 'Tokens in circulation', 'Per token price']]
                    const tokenTransactions = publicAccount[tokenID].transactions.filter(x => x.unit === tokenID)
                    let _tokenSupply = 0;
                    tokenTransactions.forEach(async transaction => {
                        let txnDate = new Date(transaction.timestamp)
                        _tokenSupply = _tokenSupply - transaction.amount
                        let _tokenPrice = Math.log10(1 + _tokenSupply)
                        dataArray.push([txnDate.toLocaleTimeString('en-us'), _tokenSupply, _tokenPrice])
                    })
                    await chart( tokenID, dataArray ).then( () => {
                        console.log( 'token market opened' )
                    } )
                })
            } )

            //for each token fetch actual tweet data

            let tweets = document.getElementsByClassName("tweet");

            for (let i=0; i < tweets.length; i++ ) {
                let id = tweets[i].id
                let tweet = tweets[i]
                twttr.widgets.createTweet(
                    id, tweet,
                    {
                        conversation : 'none',    // or all
                        cards        : 'hidden',  // or visible
                        linkColor    : 'bisque', // default is blue
                        theme        : 'light'    // or dark
                    });
            }
        }

        async function chart (tokenID, dataArray) {
            google.charts.load('current', {'packages':['corechart']});
            google.charts.setOnLoadCallback(drawChart);

            function drawChart() {

                console.log(dataArray)
                let data = google.visualization.arrayToDataTable(dataArray);

                let options = {
                    title: 'Market data',
                    titleTextStyle: {fontSize: 16},
                    fontName: 'voces',
                    hAxis: {minValue: dataArray[0][0], titleTextStyle: {color: '#333'}, format: 'HH:MM', textStyle: {fontSize: '9'}},
                    vAxis: { minValue: 0},
                    series: {
                        0: {
                            targetAxisIndex: 0,
                            color: 'gray',
                            areaOpacity: '.2',
                        },
                        1: {
                            targetAxisIndex: 1,
                            color: 'orange',
                        }
                    },
                    vAxes: {
                        0: {
                            title:'Volume',
                            format: '###',
                            minValue: dataArray[0][2],
                            textStyle: {fontSize: '10', italic: 'false'}
                        },
                        1: {
                            title:'💲Price',
                            format: '#.##',
                            minValue: dataArray[0][1],
                            textStyle: {fontSize: '10', italic: 'false'}
                        }
                    },

                    chartArea: {width: 500, height: 300, left: '10%', right: '10%'},
                    backgroundColor: '#f1f1f1',
                };

                document.getElementById('tokenModal').style.display ='block'
                document.getElementById( "tokenModal" ).style.visibility = 'visible'

                if (document.getElementById('tokenID')){
                    document.getElementById('tokenID').remove()
                }
                if (document.getElementById('buy')){
                    document.getElementById('buy').remove()
                }
                if (document.getElementById('sell')){
                    document.getElementById('sell').remove()
                }
                if (document.getElementById('marketTip')){
                    document.getElementById('marketTip').remove()
                }

                let i = document.createElement( 'div' )
                i.setAttribute( "id" , 'tokenID' )
                i.setAttribute("class", "charts")
                i.setAttribute( "style" , "width: 450px; height: 450px;" )
                document.getElementById( 'tokenDetail' ).appendChild( i )
                let k = document.createElement( 'button' )
                k.setAttribute( "id" , "sell" )
                k.appendChild( document.createTextNode( "Sell" ) )
                document.getElementById( 'tokenDetail' ).appendChild( k )
                let j = document.createElement( 'button' )
                j.setAttribute( "id" , "buy" )
                j.appendChild( document.createTextNode( "Buy" ) )
                document.getElementById( 'tokenDetail' ).appendChild( j )
                let l = document.createElement('p')
                l.setAttribute('id', 'marketTip')
                l.setAttribute('style', 'display: inline-block; font-size: 10px')
                l.appendChild(document.createTextNode('Tokens are automatically minted and burned from token\'s market pool. No counterparty needed.'))
                document.getElementById( 'tokenDetail' ).appendChild( l )

                document.getElementById( 'sell' ).addEventListener( 'click' , async () => {
                    document.getElementById('tokenSell').style.display='block'
                    document.getElementById('paymentFromTokenID').value = tokenID
                } )

                document.getElementById( 'buy' ).addEventListener( 'click' , async () => {
                    document.getElementById('tokenPurchase').style.display='block'
                    document.getElementById('paymentToTokenID').value = tokenID
                } )

                let chart = new google.visualization.AreaChart(i);
                chart.draw(data, options);
            }
        }

        async function friendVerificationRequest( smartID ) {
            let i = document.createElement ( 'button' )
            i.setAttribute ( "id" , smartID )
            i.setAttribute ( "value" , smartID )
            i.setAttribute ( "class" , "verificationRequests" )
            i.appendChild ( document.createTextNode ( smartID ) )
            document.getElementById ( 'friendRequests' ).appendChild ( i )
            document.getElementById ( smartID ).addEventListener ( 'click' , () => {
                openAccount ( smartID ).then ( () => {
                    console.log ( 'friend request opened' )
                } )
            } )
        }

        async function networkVerificationRequest( smartID ) {
            let i = document.createElement ( 'button' )
            i.setAttribute ( "id" , smartID )
            i.setAttribute ( "value" , smartID )
            i.setAttribute ( "class" , "verificationRequests" )
            i.appendChild ( document.createTextNode ( smartID ) )
            document.getElementById ( 'networkRequests' ).appendChild ( i )
            document.getElementById ( smartID ).addEventListener ( 'click' , () => {
                openAccount ( smartID ).then ( () => {
                    console.log ( 'network request opened' )
                } )
            } )
        }

        async function downloadCredentials( ) {

            let credentials = {
                [ orbitdb.id ] : {
                    privateKey : await orbitdb.keystore.exportPrivateKey( orbitdb.key ) ,
                    publicKey : await orbitdb.keystore.exportPublicKey( orbitdb.key ) ,
                } ,
                smartID : mySmartID ,
                account : myAddress ,
                signature : await orbitdb.keystore.sign( orbitdb.key , mySmartID )
            }
            let a = document.createElement ( "a" )
            let file = new Blob ( [ JSON.stringify( credentials )  ] , { type : 'text/plain' } );
            a.href = URL.createObjectURL ( file );
            a.download = 'smartX-credentials.txt';
            a.click ()
        }

        async function socialUrl (smartID) {
            console.log('loading twitter value...')
            let socialUrl;
            if (myAccount.get('social') === undefined || !myAccount.get('social').twitter) {
                let socialUrl = 'https://twitter.com/intent/tweet/?text=Hey ' +
                    document.getElementById( 'onboardingSubmit' ).value + ' , can you please verify my smartID? \n'
                    + ' \n smartXbot verify ' + document.getElementById( 'onboardingSmartID' ).value
                return socialUrl
            } else {
                let socialUrl = 'https://twitter.com/' + myAccount.get('social').twitter
                return socialUrl
            }
        }

        document.getElementById ( 'open' ).addEventListener ( 'click' , async () => {
            await openAccount ( document.getElementById ( 'findID' ).value )
        } )

        document.getElementById ( 'downloadCredentials' ).addEventListener ( 'click' , async () => {
            await downloadCredentials()
        } )

        document.getElementById ( 'forcedSync' ).addEventListener ( 'click' , async () => {
            document.getElementById('splashScreen').style.display = 'block'
            await forcedSync().then(() => {
                document.getElementById('splashScreen').style.display = 'none'
                alert('account successfully synced')
            })
        } )

        document.getElementById('paymentSent').addEventListener('click', async () => {
            if (Number(document.getElementById('paymentAmount').value) > 0) {
                await sendValue( document.getElementById( 'paymentTo' ).value ,
                    Number( document.getElementById( 'paymentAmount' ).value ) ,
                    'direct transfer to ' + document.getElementById( 'paymentTo' ).value , 'smartCoin' ).then( () => alert(
                    'Your payment request has been processed!'
                ) )
            } else {
                alert('Your request could not be processed. Amount sent has to be more than 0')
            }
        })

        document.getElementById('paymentSentToTokenID').addEventListener('click', async () => {

            let tokenID = document.getElementById('paymentToTokenID').value
            let quantity = Number(document.getElementById('paymentAmountToTokenID').value)

            let currentTokenCount = await tokenSupply(tokenID, tokenID)
            let newTokenCount = currentTokenCount + quantity
            let totalInvestment = await integrate(currentTokenCount, newTokenCount, 0.1)
            let markUp = 100 / (100 * (1 - publicAccount[tokenID].creators[0].royalty - publicAccount[tokenID].issuer[0].fee))
            let actualInvestment = totalInvestment * markUp

            if (Number(document.getElementById('paymentAmountToTokenID').value) > 0) {
                await sendValue( tokenID, actualInvestment , quantity + ' tokens bought of ' + tokenID, 'smartCoin' ).then( () =>
                    alert( 'Your purchase request has been processed!' ) )
            } else {
                alert('Your request could not be processed. Amount bought has to be more than 0')
            }
        })

        document.getElementById('paymentSentFromTokenID').addEventListener('click', async () => {

            let tokenID = document.getElementById('paymentFromTokenID').value
            let quantity = Number(document.getElementById('paymentAmountFromTokenID').value)

            if (Number(document.getElementById('paymentAmountFromTokenID').value) > 0) {
                await sendValue( tokenID, quantity , quantity + ' tokens sold of ' + tokenID , tokenID ).then( () => alert(
                    'Your sell request has been processed!' ) )
            } else {
                alert('Your request could not be processed. Amount sold has to be more than 0')
            }
        })

        document.getElementById('tokenCreated').addEventListener('click', async () => {
            let url = document.getElementById('tokenUrl').value.toString()
            console.log(url)
            if (url.startsWith('https://twitter.com/')) {
                let urlString = url.replace( 'https://twitter.com/' , '' )
                let extractValues = urlString.split( '/' )
                let creator = extractValues[ 0 ].toLowerCase()
                let urlID = extractValues[ 2 ]

                let dataObj = {
                    for: publicSmartID,
                    ID : creator ,
                    urlID : urlID ,
                    url : url ,
                    content : url ,
                    type : 'tokenCreation' ,
                }
                console.log(dataObj)
                await orbitdb._pubsub.publish( 'smartX' , dataObj )
                alert('Your token has been created! Please refresh your page to see it live in the market!')
            } else {
                alert('Sorry, you can only tokenize tweets for now!')
            }
        })

        let socialShares = document.querySelectorAll(".js-social-share");
        if (socialShares) {
            [].forEach.call(socialShares, function(anchor) {
                anchor.addEventListener("click", async function(e) {
                    e.preventDefault();

                    if (this.id === 'sendTweet') {
                        const href = 'https://twitter.com/intent/tweet/?text=Hey ' +
                            document.getElementById( 'onboardingSubmit' ).value + ' , can you please verify my smartID? \n'
                            + ' cc smartXbot verify ' + document.getElementById( 'onboardingSmartID' ).value

                        if (!document.getElementById( 'onboardingSubmit' ).value) {
                            alert( "Please enter twitter ID of verifier and tweet to them." )
                        } else if (!document.getElementById( "blobdata" ).value && myAccount.get('proof') === null ) {
                            alert( 'Please record a video selfie to submit verification request.' )
                        } else {
                            if (await myAccount.get('proof') !== undefined) {
                                if (myAccount.get( 'proof' ) === null) {
                                    myAccount.get( 'state' ).proof = await myAccount.put( 'proof' , document.getElementById( "blobdata" ).value )
                                    await myAccount.put( 'state' , myAccount.get( 'state' ) )
                                        .then( async ( hash ) => await sendStateToPublicAccount( hash ).then( () => {
                                            console.log( 'updated video selfie proof in own and public account' )
                                        } ) )
                                }
                                const twitterID = document.getElementById( 'onboardingSubmit' ).value.split('@')[1].toLowerCase()
                                const friendID = await twitterIDToSmartID(twitterID)
                                if ( friendID !== undefined && publicAccount.index[ friendID ].status === 'verified') {
                                    windowPopup( href , 600 , 400 );
                                } else {
                                    alert( 'Your verification request could not be submitted. ' +
                                        'Please submit your request to a friend already verified on smartX using their @twitterhandle.' )
                                }
                            } else {
                                alert('smartX\'s network is facing higher than usual traffic at the moment. ' +
                                    'Please keep this tab open and try again in a few minutes. Sorry for the inconvenience.')
                            }
                        }
                    } else if (this.id === 'shareTweet') {
                        const href = `https://twitter.com/intent/tweet/?text=Yay! I just created my smartID and became an e-citizen of the virtual, self-sovereign economy, smartX! \n I can now verify you so claim your smartID and smartCoins right away and join me in the movement to simplify and democratize markets.`
                        windowPopup( href , 600 , 400 );
                    }
                });
            })
        }

        function windowPopup(url, width, height) {
            // Calculate the position of the popup so
            // it’s centered on the screen.
            let left = (screen.width / 2) - (width / 2);
            //     let  top = (screen.height / 2) - (height / 2);
            window.open(
                url,
                "",
                "menubar=no,toolbar=no,resizable=yes,scrollbars=yes,width=" + width + ",height=" + height + ",top=" + top + ",left=" + left
            );
        }

        await orbitdb._pubsub.publish( 'smartX' , {type: 'requestingPublicEntries'} )
        await openAccount( mySmartID )

        setTimeout(async () => {
            if (publicAccount) {
                await checkAccount()
                await openAccount( mySmartID )
                await displayRequests()
                await showTokens()
            } else {
                await orbitdb._pubsub.publish( 'smartX' , {type: 'requestingPublicEntries'} )
                setTimeout(async () => {
                    if (publicAccount) {
                        await checkAccount()
                        await openAccount( mySmartID )
                        await displayRequests()
                        await showTokens()
                    } else {
                        await orbitdb._pubsub.publish( 'smartX' , {type: 'requestingPublicEntries'} )
                        setTimeout(async () => {
                            if (publicAccount) {
                                await checkAccount()
                                await openAccount( mySmartID )
                                await displayRequests()
                                await showTokens()
                            } else {
                                await orbitdb._pubsub.publish( 'smartX' , {type: 'requestingPublicEntries'} )
                                    setTimeout(async () => {
                                        if (publicAccount) {
                                            await checkAccount()
                                            await openAccount( mySmartID )
                                            await displayRequests()
                                            await showTokens()
                                        } else {
                                            alert('Syncing of account seems to be taking a bit longer than expected due to high traffic. ' +
                                                'Please keep this tab open and check back again in couple of mins. ' +
                                                'Sorry for the inconvenience.')
                                        }
                                    }, 20000)
                            }
                        }, 15000)
                    }
                }, 10000)
            }
        }, 5000)

        const fullPublicAccount = await orbitdb.open( `/orbitdb/${publicSmartID}/publicAccount` )

        fullPublicAccount.events.on ( 'replicated' , async  () => {
            if (!fullPublicAccount.get('index') || !fullPublicAccount.get('verifiedMembers') || !fullPublicAccount.get('socialServices')
                || !fullPublicAccount.get('verifiedMembers').members || !fullPublicAccount.get('socialServices').oracles) {
                return
            }

            if (!publicAccount) {
                publicAccount = Object.entries( fullPublicAccount )[ 13 ][ 1 ][ '_index' ]
                await checkAccount()
                await openAccount( mySmartID )
                await displayRequests()
                await showTokens()
            }

            publicAccount = Object.entries( fullPublicAccount )[ 13 ][ 1 ][ '_index' ]
            console.log('public account synced from main and updated locally: ', publicAccount)

            if (publicAccount.index[mySmartID] === undefined) {
                console.log('peerID-smartID mapping not present so adding...')
                let dataObj = {
                    from : mySmartID ,
                    fromPeer : orbitdb.id ,
                    to : publicSmartID ,
                    type : 'index' ,
                }
                await orbitdb._pubsub.publish( 'smartX' , dataObj )
            }

            if (publicAccount && myAccount.get( 'smartID' ) !== undefined && publicAccount[mySmartID] === undefined) {
                sendStateToPublicAccount().then(() => console.log('state not updated in public account so sent again'))
            }
        } )

    } )
}